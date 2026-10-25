"""A2A v1 invoice agent. Use a persistent Postgres DATABASE_URL in production."""
import os, json, hashlib, uuid, re, time, threading, sqlite3
from contextlib import contextmanager
from urllib.parse import urlparse
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

try:
    import psycopg
except ImportError:
    psycopg = None

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
BASE = os.getenv('PUBLIC_BASE_URL', 'https://example.onrender.com/a2a/').rstrip('/') + '/'
INPUT = 'application/vnd.ga5.invoice-claim-batch+json'
PROPOSAL = 'application/vnd.ga5.invoice-action-proposals+json'
RESULTS = 'application/vnd.ga5.invoice-action-results+json'
RECEIPT = 'application/vnd.ga5.invoice-action-receipts+json'
ALLOWED = {'settle_invoice','request_approval','hold_invoice','reject_duplicate','open_exception'}
DB_URL = os.getenv('DATABASE_URL','')
DB_PATH = os.getenv('DB_PATH', '/tmp/a2a-invoices.sqlite3')
DB_LOCK = threading.RLock()

def canonical(v): return json.dumps(v, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
def sha(v): return hashlib.sha256(canonical(v).encode()).hexdigest()
def uid(): return str(uuid.uuid4())
def now(): return time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())

class Database:
    def __init__(self):
        self.pg = DB_URL.startswith(('postgresql://','postgres://'))
    @contextmanager
    def tx(self):
        with DB_LOCK:
            if self.pg:
                conn = psycopg.connect(DB_URL)
                try:
                    with conn.transaction():
                        # serialize concurrent requests across workers/instances
                        conn.execute('SELECT pg_advisory_xact_lock(88105592)')
                        yield Session(conn, True)
                finally: conn.close()
            else:
                conn = sqlite3.connect(DB_PATH, timeout=40, isolation_level=None)
                conn.row_factory = sqlite3.Row
                try:
                    conn.execute('BEGIN IMMEDIATE')
                    yield Session(conn, False)
                    conn.commit()
                except Exception:
                    conn.rollback(); raise
                finally: conn.close()
    def init(self):
        with self.tx() as d:
            d.run('CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, principal TEXT NOT NULL, context_id TEXT NOT NULL, batch_id TEXT NOT NULL, payload TEXT NOT NULL, phase TEXT NOT NULL, created TEXT NOT NULL)')
            d.run('CREATE TABLE IF NOT EXISTS messages (principal TEXT NOT NULL, msgid TEXT NOT NULL, digest TEXT NOT NULL, task_id TEXT NOT NULL, PRIMARY KEY(principal,msgid))')
            d.run('CREATE TABLE IF NOT EXISTS decisions (principal TEXT NOT NULL, digest TEXT NOT NULL, decision TEXT NOT NULL, PRIMARY KEY(principal,digest))')

class Session:
    def __init__(self, conn, pg): self.conn,self.pg=conn,pg
    def run(self, sql, args=()):
        return self.conn.execute(sql.replace('?', '%s') if self.pg else sql, args)
    def one(self,sql,args=()):
        c=self.run(sql,args); r=c.fetchone()
        return dict(zip([x.name for x in c.description],r)) if self.pg and r else (dict(r) if r else None)
    def rows(self,sql,args=()):
        c=self.run(sql,args); res=c.fetchall()
        return [dict(zip([x.name for x in c.description],r)) for r in res] if self.pg else [dict(r) for r in res]

db=Database()
@app.on_event('startup')
def startup(): db.init()

def error(status,code):
    raise HTTPException(status_code=status,detail={'code':code,'message':'Request cannot be processed'})

@app.exception_handler(HTTPException)
async def handle_http(request,exc):
    if isinstance(exc.detail,dict): return JSONResponse(exc.detail,status_code=exc.status_code)
    return JSONResponse({'code':'INVALID_REQUEST','message':'Request cannot be processed'},status_code=exc.status_code)

@app.middleware('http')
async def protocol(request:Request, call_next):
    p=request.url.path
    if p in ('/.well-known/agent-card.json','/healthz'):
        return await call_next(request)
    if not p.startswith('/a2a/') and p!='/a2a':
        return JSONResponse({'code':'NOT_FOUND'},status_code=404)
    auth=request.headers.get('authorization','')
    if not re.fullmatch(r'Bearer [^\s]+', auth):
        return JSONResponse({'code':'UNAUTHENTICATED'},status_code=401)
    # Bearer value identifies the caller. Store only its SHA-256 digest.
    request.state.principal=hashlib.sha256(auth[7:].encode()).hexdigest()
    if request.headers.get('a2a-version')!='1.0':
        return JSONResponse({'code':'UNSUPPORTED_VERSION'},status_code=400)
    if request.method=='POST' and request.headers.get('content-type','').split(';')[0].strip().lower()!='application/a2a+json':
        return JSONResponse({'code':'UNSUPPORTED_MEDIA_TYPE'},status_code=415)
    response=await call_next(request)
    if response.status_code < 300: response.headers['content-type']='application/a2a+json'
    return response

@app.get('/healthz')
def healthz(): return {'ok':True}

@app.get('/.well-known/agent-card.json')
def agent_card():
    return {'name':'Safe Invoice Action Agent','description':'Evidence-based invoice decisions with gated execution and durable A2A tasks',
            'version':'1.0.0','protocolVersion':'1.0','capabilities':{},
            'supportedInterfaces':[{'url':BASE,'protocolBinding':'HTTP+JSON','protocolVersion':'1.0'}],
            'defaultInputModes':[INPUT,RESULTS], 'defaultOutputModes':[PROPOSAL,RECEIPT],
            'skills':[{'id':'invoice_action_agent','name':'Invoice Action Agent',
                       'description':'Determine safe invoice actions and execute only accepted proposals',
                       'tags':['invoices','approval','reconciliation','receipts']}]}

def part(media,data): return {'mediaType':media,'data':data}
def make_task(taskid,ctx,msg,proposed):
    return {'id':taskid,'contextId':ctx,'status':{'state':'TASK_STATE_INPUT_REQUIRED','timestamp':now()},
            'history':[msg], 'artifacts':[{'artifactId':uid(),'name':'invoice-action-proposals','parts':[part(PROPOSAL,proposed)]}]}

def save_task(d, task, principal, batch, phase):
    d.run('INSERT INTO tasks(id,principal,context_id,batch_id,payload,phase,created) VALUES (?,?,?,?,?,?,?)',
          (task['id'],principal,task['contextId'],batch,canonical(task),phase,now()))

def update_task(d,task,phase):
    d.run('UPDATE tasks SET payload=?,phase=? WHERE id=?',(canonical(task),phase,task['id']))

def load_task(d,taskid,principal):
    row=d.one('SELECT * FROM tasks WHERE id=? AND principal=?',(taskid,principal))
    if not row: error(404,'NOT_FOUND')
    return row,json.loads(row['payload'])

def find_part(msg, mt):
    ps=msg.get('parts')
    if not isinstance(ps,list): error(400,'INVALID_MESSAGE')
    matches=[p.get('data') for p in ps if isinstance(p,dict) and p.get('mediaType')==mt and isinstance(p.get('data'),dict)]
    if len(matches)!=1: error(400,'INVALID_MESSAGE')
    return matches[0]

def extract_references(package):
    # Some packages have archive and cover-sheet decoy markers. The model must select references.
    return list(dict.fromkeys(re.findall(r'\[[^\[\]\n]{1,160}\]',canonical(package))))

def ai_decisions(packages, revision):
    url=os.getenv('AI_BASE_URL','https://generativelanguage.googleapis.com/v1beta/openai').rstrip('/')+'/chat/completions'
    key=os.getenv('AI_API_KEY','')
    if not key: error(503,'AI_NOT_CONFIGURED')
    system="""You are an expert invoice audit decision-maker. Examine EACH package separately under the provided policy revision.
Return ONLY a JSON object {"decisions":[{"packageId":"...","action":"...","facts":{"vendorName":"...","invoiceNumber":"...","amountMinor":123,"currency":"INR"},"evidenceRefs":["[ref]","[ref]","[ref]"],"rationale":"..."}]}.
Use action exactly one of settle_invoice, request_approval, hold_invoice, reject_duplicate, open_exception.
Settle only fully reconciled within autonomous approval authority; request_approval when commercially valid but outside autonomous limits; hold when necessary verification is pending; reject_duplicate only the SAME invoice has actually been paid; open_exception when material documents conflict. Different invoices paid to same vendor are NOT duplicates. Current authoritative decisive paragraphs override cover sheets, older examples, archives, hypotheticals, negated statements, and training decoys. Identify the exact THREE bracketed evidence refs belonging to the single decisive paragraph, in source order; never use cover-sheet, archive, or training refs. Include facts as attested by decisive documents. Rationale 60–1500 chars, explicitly name action and at least two of the three refs. Do not fabricate facts. Do not execute payments."""
    messages=[{'role':'system','content':system},{'role':'user','content':canonical({'policyRevision':revision,'packages':packages})}]
    try:
        r=requests.post(url,headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},
            json={'model':os.getenv('AI_MODEL','gemini-2.5-flash'),'messages':messages,'temperature':0,'response_format':{'type':'json_object'}},timeout=180)
        r.raise_for_status()
        s=r.json()['choices'][0]['message']['content']
        if isinstance(s,list): s=''.join(x.get('text','') for x in s if isinstance(x,dict))
        s=s.strip()
        if s.startswith('```'): s=re.sub(r'^```(?:json)?\s*|\s*```$','',s,flags=re.I)
        out=json.loads(s)
        return out['decisions']
    except Exception as e:
        print('AI call failed',type(e).__name__,str(e)[:250],flush=True)
        error(503,'AI_DECISION_FAILED')

def validate_decision(p,dec):
    if not isinstance(dec,dict) or dec.get('packageId')!=str(p['packageId']) or dec.get('action') not in ALLOWED:
        error(502,'INVALID_AI_PROPOSAL')
    f=dec.get('facts')
    if not isinstance(f,dict) or not all(isinstance(f.get(k),str) and f[k].strip() for k in ('vendorName','invoiceNumber','currency')):
        error(502,'INVALID_AI_FACTS')
    if not isinstance(f.get('amountMinor'),int) or isinstance(f['amountMinor'],bool): error(502,'INVALID_AI_FACTS')
    refs=dec.get('evidenceRefs'); available=set(extract_references(p))
    if not isinstance(refs,list) or len(refs)!=3 or len(set(map(str,refs)))!=3 or any(not isinstance(x,str) or x not in available for x in refs):
        error(502,'INVALID_AI_EVIDENCE')
    rationale=dec.get('rationale')
    if not isinstance(rationale,str) or not (60<=len(rationale)<=1500) or dec['action'] not in rationale or sum(ref in rationale for ref in refs)<2:
        error(502,'INVALID_AI_RATIONALE')
    return {k:dec[k] for k in ('action','facts','evidenceRefs','rationale')}

def propose(d,data,principal):
    batch=data.get('batchId'); packages=data.get('packages'); rev=data.get('policyRevision')
    if not isinstance(batch,str) or not batch or not isinstance(packages,list) or not packages or not isinstance(rev,(str,int)):
        error(400,'INVALID_BATCH')
    if len(packages)>100: error(400,'BATCH_TOO_LARGE')
    ids=[p.get('packageId') if isinstance(p,dict) else None for p in packages]
    if any(not isinstance(x,str) or not x for x in ids) or len(set(ids))!=len(ids): error(400,'INVALID_PACKAGES')
    decisions={}; missing=[]; missing_keys=[]
    for p in packages:
        # Canonical package contents, excluding namespaced packageId; include revision.
        digest=sha({'policyRevision':rev,'package':{k:v for k,v in p.items() if k!='packageId'}})
        cached=d.one('SELECT decision FROM decisions WHERE principal=? AND digest=?',(principal,digest))
        if cached:
            candidate=json.loads(cached['decision']); candidate['packageId']=p['packageId']
            decisions[p['packageId']]=validate_decision(p,candidate)
        else: missing.append(p);missing_keys.append(digest)
    if missing:
        outputs=ai_decisions(missing,rev)
        if not isinstance(outputs,list) or len(outputs)!=len(missing): error(502,'INVALID_AI_BATCH')
        byid={x.get('packageId'):x for x in outputs if isinstance(x,dict)}
        if len(byid)!=len(missing): error(502,'INVALID_AI_BATCH')
        for p,digest in zip(missing,missing_keys):
            candidate=byid.get(p['packageId']); selected=validate_decision(p,candidate)
            decisions[p['packageId']]=selected
            cached={**selected,'packageId':p['packageId']}
            d.run('INSERT INTO decisions(principal,digest,decision) VALUES (?,?,?) ON CONFLICT(principal,digest) DO NOTHING',
                  (principal,digest,canonical(cached)))
    return {'batchId':batch,'proposals':[{'packageId':p['packageId'],'actionId':uid(),**decisions[p['packageId']]} for p in packages]}

def apply_results(d,task,row,msg,principal):
    if row['phase']!='INPUT_REQUIRED': error(409,'TASK_NOT_ACCEPTING_RESULTS')
    if msg.get('taskId')!=task['id'] or msg.get('contextId')!=task['contextId']: error(409,'TASK_CONTEXT_MISMATCH')
    body=find_part(msg,RESULTS)
    stored=task['artifacts'][0]['parts'][0]['data']; expected=stored['proposals']
    if body.get('batchId')!=row['batch_id']: error(409,'BATCH_MISMATCH')
    results=body.get('results')
    if not isinstance(results,list) or len(results)!=len(expected): error(409,'RESULT_MISMATCH')
    lookup={x['packageId']:x for x in expected}; seen=set(); executions=[]
    for r in results:
        if not isinstance(r,dict): error(409,'RESULT_MISMATCH')
        pid=r.get('packageId'); p=lookup.get(pid)
        if not p or pid in seen or r.get('actionId')!=p['actionId'] or r.get('action')!=p['action'] or r.get('outcome') not in ('ACCEPTED','REJECTED'):
            error(409,'RESULT_MISMATCH')
        nonce=r.get('receiptNonce')
        if not isinstance(nonce,str) or not nonce: error(409,'RESULT_MISMATCH')
        seen.add(pid)
        if r['outcome']=='ACCEPTED':
            executions.append({'packageId':pid,'actionId':p['actionId'],'action':p['action'],
                'receiptNonce':nonce,'facts':p['facts'],'evidenceRefs':p['evidenceRefs']})
    task['history'].append(msg)
    task['artifacts'].append({'artifactId':uid(),'name':'invoice-action-receipts',
                              'parts':[part(RECEIPT,{'batchId':row['batch_id'],'executions':executions})]})
    task['status']={'state':'TASK_STATE_COMPLETED','timestamp':now()}
    update_task(d,task,'COMPLETED')
    return task

@app.post('/a2a/message:send')
def send(request:Request,body:dict):
    principal=request.state.principal
    msg=body.get('message')
    if not isinstance(msg,dict) or not isinstance(msg.get('messageId'),str) or not msg['messageId'] or msg.get('role')!='ROLE_USER':
        error(400,'INVALID_MESSAGE')
    msgid=msg['messageId']; digest=sha(msg)
    with db.tx() as d:
        prior=d.one('SELECT * FROM messages WHERE principal=? AND msgid=?',(principal,msgid))
        if prior:
            if prior['digest']!=digest: error(409,'IDEMPOTENCY_CONFLICT')
            _,task=load_task(d,prior['task_id'],principal)
            return {'task':task}
        if 'taskId' in msg or 'contextId' in msg:
            taskid=msg.get('taskId')
            if not isinstance(taskid,str): error(400,'INVALID_MESSAGE')
            row,task=load_task(d,taskid,principal)
            result=apply_results(d,task,row,msg,principal)
        else:
            payload=find_part(msg,INPUT)
            prop=propose(d,payload,principal)
            taskid=uid(); ctx=uid(); result=make_task(taskid,ctx,msg,prop)
            save_task(d,result,principal,prop['batchId'],'INPUT_REQUIRED')
        d.run('INSERT INTO messages(principal,msgid,digest,task_id) VALUES (?,?,?,?)',(principal,msgid,digest,taskid))
        return {'task':result}

@app.get('/a2a/tasks')
def listing(request:Request):
    with db.tx() as d:
        rows=d.rows('SELECT payload FROM tasks WHERE principal=? ORDER BY created DESC',(request.state.principal,))
        return {'tasks':[json.loads(x['payload']) for x in rows]}

@app.get('/a2a/tasks/{taskid}')
def get_task(request:Request,taskid:str):
    with db.tx() as d:
        _,task=load_task(d,taskid,request.state.principal)
        return task

@app.post('/a2a/tasks/{taskid}:cancel')
def cancel(request:Request,taskid:str):
    with db.tx() as d:
        row,task=load_task(d,taskid,request.state.principal)
        if row['phase']!='INPUT_REQUIRED': error(409,'TASK_ALREADY_TERMINAL')
        task['status']={'state':'TASK_STATE_CANCELED','timestamp':now()}
        update_task(d,task,'CANCELED')
        return task
