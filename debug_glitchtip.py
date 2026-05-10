import os
import urllib.error
import urllib.request

# 1. Get DSN
dsn = os.getenv('SENTRY_DSN', '')
print('--- DIAGNOSTIC START ---')
print(f'DSN Found: {dsn}')

if not dsn or 'glitchtip_web' not in dsn:
    print('❌ CRITICAL ERROR: DSN is invalid or using localhost instead of glitchtip_web')
    print('Fix your .env file to use: http://KEY@glitchtip_web:8000/1')
    exit()

# 2. Extract Data
try:
    proto_key, rest = dsn.split('@')
    host_port, project_id = rest.split('/')
    key = proto_key.split('://')[1]
    url = f'http://{host_port}/api/{project_id}/store/'
except Exception as e:
    print(f'❌ ERROR parsing DSN: {e}')
    exit()

# 3. Create Payload (Simple JSON)
payload = b'{"message": "Manual Python Test", "platform": "python", "level": "error"}'

# 4. Send Request
print(f'Target URL: {url}')
req = urllib.request.Request(url, data=payload, method='POST')
req.add_header('X-Sentry-Auth', f'Sentry sentry_key={key}, sentry_version=7, sentry_client=custom/1.0')
req.add_header('Content-Type', 'application/json')

try:
    resp = urllib.request.urlopen(req)
    print(f'✅ SUCCESS! Status: {resp.status}')
    print('Server accepted the event. Check GlitchTip dashboard.')
except urllib.error.HTTPError as e:
    print(f'❌ FAILED! Status: {e.code}')
    print('--- SERVER REASON (The Missing Link) ---')
    print(e.read().decode())
    print('----------------------------------------')
except Exception as e:
    print(f'❌ CONNECTION ERROR: {e}')
