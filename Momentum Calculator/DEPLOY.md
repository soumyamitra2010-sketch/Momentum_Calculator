# Deployment Guide - PythonAnywhere (Free Tier)

## Why PythonAnywhere?
- ✅ **Free Flask hosting** (one web app)
- ✅ **Scheduled tasks** (cron jobs)
- ✅ **Always-on workers** (for continuous data updates)
- ✅ **Indian timezone support** (set TZ=Asia/Kolkata)
- ✅ **Mobile accessible** - share URL anywhere

---

## Step 1: Sign Up
1. Go to https://www.pythonanywhere.com/
2. Sign up for **Beginner (Free)** account
3. Choose username (this becomes your URL: `username.pythonanywhere.com`)

---

## Step 2: Upload Files

### Option A: Via GitHub
```bash
# In PythonAnywhere Bash console:
git clone https://github.com/yourusername/Momentum_Calculator.git
cd Momentum_Calculator
```

### Option B: Via Upload
1. Go to **Files** tab
2. Navigate to `/home/yourusername/`
3. Click **Upload** button for each file:
   - `app.py`
   - `engine.py`
   - `etf_data.py`
   - `index.html`
   - `wsgi.py`
   - `refresh_data.py`
   - `data_worker.py`

---

## Step 3: Install Dependencies

In **Bash console**:
```bash
pip3 install --user flask pandas requests urllib3
```

---

## Step 4: Configure Web App

1. Go to **Web** tab
2. Click **Add a new web app**
3. Choose **Flask**
4. Python version: **3.10** (or latest available)
5. Path to your Flask app: `/home/yourusername/Momentum_Calculator/wsgi.py`
6. Click **Next**

### Web App Settings:
- **Source code**: `/home/yourusername/Momentum_Calculator`
- **Working directory**: `/home/yourusername/Momentum_Calculator`
- **WSGI file**: `/var/www/username_pythonanywhere_com_wsgi.py` (auto-generated)

### Update WSGI file:
Replace the auto-generated WSGI with:
```python
import sys
path = '/home/yourusername/Momentum_Calculator'
if path not in sys.path:
    sys.path.append(path)

from wsgi import application
```

---

## Step 5: Set Timezone (IMPORTANT)

For Indian market hours, set timezone to IST:

1. Go to **Consoles** → Start new **Bash** console
2. Run:
```bash
# Add to ~/.bashrc
echo 'export TZ=Asia/Kolkata' >> ~/.bashrc
source ~/.bashrc
```

3. Or set in **Web** tab under **Environment variables**:
   - Name: `TZ`
   - Value: `Asia/Kolkata`

---

## Step 6: Setup Auto Data Refresh

### Option A: Always-on Task (Recommended)
Runs continuously, refreshes data during trading hours.

1. Go to **Tasks** → **Always-on tasks**
2. Click **Create always-on task**
3. Command: `python /home/yourusername/Momentum_Calculator/data_worker.py`
4. Click **Create**

### Option B: Scheduled Task (Hourly during trading)
Runs only at scheduled times.

1. Go to **Tasks** → **Scheduled tasks**
2. Create multiple tasks (hourly from 9 AM to 4 PM IST):

| Time (IST) | Cron Schedule | Command |
|------------|---------------|---------|
| 9:15 AM | `15 9 * * 1-5` | `python /home/yourusername/Momentum_Calculator/refresh_data.py` |
| 10:15 AM | `15 10 * * 1-5` | `python /home/yourusername/Momentum_Calculator/refresh_data.py` |
| 11:15 AM | `15 11 * * 1-5` | `python /home/yourusername/Momentum_Calculator/refresh_data.py` |
| 12:15 PM | `15 12 * * 1-5` | `python /home/yourusername/Momentum_Calculator/refresh_data.py` |
| 1:15 PM | `15 13 * * 1-5` | `python /home/yourusername/Momentum_Calculator/refresh_data.py` |
| 2:15 PM | `15 14 * * 1-5` | `python /home/yourusername/Momentum_Calculator/refresh_data.py` |
| 3:15 PM | `15 15 * * 1-5` | `python /home/yourusername/Momentum_Calculator/refresh_data.py` |

---

## Step 7: Reload and Test

1. Go to **Web** tab
2. Click **Reload** button
3. Visit: `https://yourusername.pythonanywhere.com`
4. Test on mobile browser

---

## Free Tier Limits

| Resource | Limit |
|----------|-------|
| Web app | 1 Flask app |
| Daily CPU | 100 seconds |
| Storage | 512 MB |
| Always-on task | 1 (runs 24/7) |
| Scheduled tasks | 1 per day (or multiple if short) |
| Bandwidth | Unlimited |

**Note**: Free tier has daily CPU limits. The data worker is designed to be lightweight (sleeps most of the time).

---

## Troubleshooting

### App not loading?
Check error logs: **Web** → **Logs** → **Error log**

### Data not updating?
Check task logs: **Tasks** → **Always-on tasks** → **View log**

### Timezone issues?
Verify in bash: `date` should show IST time

### Mobile display issues?
The UI is responsive. If needed, add mobile-specific CSS.

---

## Custom Domain (Optional)

1. Buy domain from GoDaddy/Namecheap (~₹500/year)
2. In PythonAnywhere **Web** tab: **Add domain name**
3. Add CNAME record pointing to `yourusername.pythonanywhere.com`

---

## Alternative: Render.com

If PythonAnywhere free tier is insufficient:

1. Sign up: https://render.com
2. Connect GitHub repo
3. Create **Web Service** → **Python**
4. Build command: `pip install -r requirements.txt`
5. Start command: `gunicorn wsgi:application`
6. For scheduled tasks, use: https://cron-job.org (free)

---

## Your URL

After deployment:
- **Web**: `https://yourusername.pythonanywhere.com`
- **Share via**: WhatsApp, Email, QR code
- **Mobile optimized**: Yes, responsive design

---

## Security Notes

1. **API exposed**: The Flask API is public on free tier
2. **No auth**: Anyone with URL can access
3. **Rate limiting**: Consider adding if needed
4. **Data**: Only ETF prices (public data)

For private access, add simple password:
```python
# In app.py, add to each route:
if request.headers.get('X-Password') != 'your-secret':
    return jsonify({"error": "Unauthorized"}), 401
```
