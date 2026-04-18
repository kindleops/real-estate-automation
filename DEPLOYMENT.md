# SMS System Deployment Checklist

## ✅ Pre-Deployment (Completed)

- [x] Database schema created in Supabase
- [x] Tables: send_queue, message_events, phone_numbers
- [x] Functions: claim_queue_jobs, mark_job_sent, mark_job_failed, unlock_stale_jobs
- [x] Indexes optimized for queue performance
- [x] Row Level Security enabled

## 📋 Deployment Steps

### 1. Get Supabase Credentials

```bash
# Go to: https://lcppdrmrdfblstpcbgpf.supabase.co
# Navigate to: Settings → API
# Copy:
# - Project URL (already have this)
# - anon public key (for client-side use)
# - service_role key (for backend use - KEEP SECRET!)
```

### 2. Set Up Local Environment

```bash
# Clone/create project directory
mkdir sms-automation && cd sms-automation

# Copy all files from the system we just built

# Install dependencies
npm install

# Create .env.local
cp .env.example .env.local

# Edit .env.local with your keys:
# - SUPABASE_SERVICE_ROLE_KEY=your_actual_key_here
# - TEXTGRID_API_KEY=your_textgrid_key
# - TEXTGRID_FROM_NUMBER=+1234567890
```

### 3. Test Locally

```bash
# Start dev server
npm run dev

# In another terminal, run tests
node test-system.js

# Test API endpoints
curl -X POST http://localhost:3000/api/messages/enqueue \
  -H 'Content-Type: application/json' \
  -d '{"to_phone_number":"+15551234567","message_body":"Test"}'

# Manually trigger queue
curl -X POST http://localhost:3000/api/queue/run
```

### 4. Deploy to Vercel

```bash
# Install Vercel CLI (if needed)
npm i -g vercel

# Login to Vercel
vercel login

# Deploy (follow prompts)
vercel

# Add environment variables via CLI or dashboard
vercel env add SUPABASE_URL
vercel env add SUPABASE_SERVICE_ROLE_KEY
vercel env add TEXTGRID_API_KEY
vercel env add TEXTGRID_FROM_NUMBER
vercel env add CRON_SECRET

# Deploy to production
vercel --prod
```

### 5. Configure TextGrid Webhooks

```bash
# Your webhook endpoints:
https://your-project.vercel.app/api/textgrid/inbound
https://your-project.vercel.app/api/textgrid/delivery

# In TextGrid dashboard:
# 1. Go to webhook settings
# 2. Set inbound SMS webhook → /api/textgrid/inbound
# 3. Set delivery status webhook → /api/textgrid/delivery
# 4. Enable webhooks
```

### 6. Verify Cron Job

```bash
# Check Vercel dashboard:
# Project → Settings → Cron Jobs
# Should see: /api/queue/run running every minute

# Monitor first runs:
# Project → Functions → Logs
# Look for queue/run executions
```

### 7. Production Testing

```bash
# Enqueue test message
curl -X POST https://your-project.vercel.app/api/messages/enqueue \
  -H 'Content-Type: application/json' \
  -d '{
    "to_phone_number": "YOUR_PHONE_NUMBER",
    "message_body": "Production test from SMS automation system"
  }'

# Wait 1 minute for cron to process

# Check database
# Go to Supabase → SQL Editor:
SELECT * FROM send_queue ORDER BY created_at DESC LIMIT 5;
SELECT * FROM message_events ORDER BY created_at DESC LIMIT 5;
```

## 🔍 Post-Deployment Verification

### Database Health Check

```sql
-- Check queue is processing
SELECT 
  queue_status,
  COUNT(*) as count,
  MAX(updated_at) as last_updated
FROM send_queue
GROUP BY queue_status;

-- Check for stuck jobs
SELECT * FROM send_queue
WHERE is_locked = true
  AND locked_at < NOW() - INTERVAL '10 minutes';

-- Check message delivery rate
SELECT 
  COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as sent,
  COUNT(*) FILTER (WHERE delivered_at IS NOT NULL) as delivered,
  COUNT(*) FILTER (WHERE failed_at IS NOT NULL) as failed
FROM message_events
WHERE created_at > NOW() - INTERVAL '24 hours';
```

### API Health Check

```bash
# Queue status
curl https://your-project.vercel.app/api/queue/run

# Response should show:
# {
#   "success": true,
#   "processed": X,
#   "sent": X,
#   "failed": 0,
#   "errors": [],
#   "duration": XXX
# }
```

### Webhook Testing

```bash
# Send test inbound (simulate TextGrid webhook)
curl -X POST https://your-project.vercel.app/api/textgrid/inbound \
  -H 'Content-Type: application/json' \
  -d '{
    "MessageSid": "test_123",
    "From": "+15551234567",
    "To": "+15559876543",
    "Body": "Test inbound message"
  }'

# Check message_events table for new inbound entry
```

## 🚨 Troubleshooting

### Issue: Queue not processing

**Check:**
1. Vercel cron logs for errors
2. Database for stuck locked jobs
3. Environment variables are set correctly

**Fix:**
```sql
-- Unlock all stuck jobs
SELECT unlock_stale_jobs(5);

-- Manually trigger queue
-- Visit: https://your-project.vercel.app/api/queue/run
```

### Issue: Messages sending but not updating status

**Check:**
1. Lock token mismatches
2. Queue job IDs

**Fix:**
```sql
-- Find jobs in "sending" state
SELECT * FROM send_queue WHERE queue_status = 'sending';

-- Reset if needed
UPDATE send_queue 
SET queue_status = 'queued', is_locked = false, lock_token = NULL
WHERE queue_status = 'sending';
```

### Issue: Webhooks not arriving

**Check:**
1. TextGrid webhook configuration
2. URL is correct (no trailing slash)
3. Webhook signature verification

**Fix:**
```bash
# Temporarily disable signature verification
# In .env.local:
TEXTGRID_VERIFY_WEBHOOKS=false

# Redeploy:
vercel --prod
```

## 📊 Monitoring Queries

```sql
-- Queue throughput (last 24 hours)
SELECT 
  DATE_TRUNC('hour', created_at) as hour,
  COUNT(*) as total,
  COUNT(*) FILTER (WHERE queue_status = 'sent') as sent,
  AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_seconds
FROM send_queue
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;

-- Active queue size
SELECT COUNT(*) FROM send_queue 
WHERE queue_status IN ('queued', 'retry');

-- Retry queue
SELECT 
  COUNT(*) as retry_count,
  AVG(retry_count) as avg_attempts
FROM send_queue
WHERE queue_status = 'retry';

-- Failed messages (need attention)
SELECT * FROM send_queue
WHERE queue_status = 'failed'
ORDER BY updated_at DESC
LIMIT 20;
```

## 🎯 Next Steps After Deployment

1. **Load Template Library**: Import your 200 SMS templates
2. **Podio Integration**: Sync contacts → phone_numbers table
3. **Auto-Responder**: Build inbound message handler logic
4. **Dashboard**: Create analytics UI
5. **Monitoring**: Set up alerts for failed messages

## 📞 Support Commands

```bash
# Check deployment status
vercel ls

# View logs
vercel logs

# Rollback deployment (if needed)
vercel rollback

# View environment variables
vercel env ls

# Run local tests
npm run dev
node test-system.js
```

---

**System Status**: ✅ Ready for deployment
**Database**: https://lcppdrmrdfblstpcbgpf.supabase.co
**Next Action**: Deploy to Vercel and configure TextGrid webhooks
