# 🚀 Open RO Dashboard - Render Deployment Guide

## ✅ What's Ready for Render

All your code has been converted to be Render-compatible:

1. **main.py** - Uses environment variables for PORT (Render default)
2. **requirements.txt** - All Python dependencies listed
3. **render.yaml** - Render configuration file
4. **dashboard.html** - Frontend (served by FastAPI)

---

## 📋 Pre-Deployment Checklist

Before deploying to Render, make sure you have:

- ✅ GitHub account (free)
- ✅ Render account (free) - https://render.com
- ✅ Your Excel file: `Open RO.xlsx` or `Open_RO.xlsx`
- ✅ Your JPG logo: `logo.jpg`

---

## 🎯 Step-by-Step Deployment Instructions

### Step 1: Prepare Files Locally

Create a folder for your project:

```
D:\open-ro-dashboard\
├── main.py              ← Download from outputs
├── dashboard.html       ← Download from outputs
├── requirements.txt     ← Download from outputs
├── render.yaml         ← Download from outputs
├── Open RO.xlsx        ← Your Excel file
├── logo.jpg            ← Your company JPG logo
└── .gitignore          ← Create this file
```

### Step 2: Create .gitignore File

Create a `.gitignore` file in your project folder with:

```
# Excel files (too large for GitHub)
*.xlsx
*.xls

# Python
__pycache__/
*.py[cod]
*.egg-info/
.env
```

### Step 3: Initialize Git Repository

Open Command Prompt in your project folder and run:

```bash
# Initialize git
git init

# Add all files (except those in .gitignore)
git add .

# Create first commit
git commit -m "Initial commit - Open RO Dashboard for Render"
```

### Step 4: Create GitHub Repository

1. Go to https://github.com/new
2. Create a new repository named: `open-ro-dashboard`
3. Follow GitHub instructions to push your code:

```bash
# Add remote origin
git remote add origin https://github.com/YOUR_USERNAME/open-ro-dashboard.git

# Rename branch if needed
git branch -M main

# Push to GitHub
git push -u origin main
```

### Step 5: Deploy on Render

1. Go to https://render.com (sign up if needed)
2. Click **"New +"** → **"Web Service"**
3. Select **"Deploy an existing Git repository"**
4. Connect your GitHub account
5. Select your `open-ro-dashboard` repository
6. Fill in the details:

   **Name:** `open-ro-dashboard`
   **Environment:** `Python`
   **Build Command:** `pip install -r requirements.txt`
   **Start Command:** `python main.py`
   **Plan:** Select your plan (Free tier available)

7. Click **"Create Web Service"**

### Step 6: Upload Excel and Logo Files

After deployment, you need to add your Excel file and logo:

**Option A - Using Render's File System (Recommended)**

1. In Render Dashboard, go to your service
2. Open the **Shell** tab
3. Create `/home/app/reel` directory if not exists
4. Upload files via SCP or similar method

**Option B - Modify Code to Use External Storage**

If files are too large, use external storage like AWS S3:

1. Store Excel file in S3 bucket
2. Modify `main.py` to download from S3 on startup
3. Update with AWS credentials in Render environment variables

### Step 7: Configure Environment Variables (Optional)

If you have sensitive data:

1. In Render Dashboard → Go to your service
2. Click **"Environment"**
3. Add any custom environment variables

---

## 🌐 Access Your Dashboard

After deployment completes:

```
Your Dashboard URL: https://your-service-name.onrender.com
```

Example: `https://open-ro-dashboard.onrender.com`

---

## 📁 File References

### main.py
- **Line 11-12:** Environment variable configuration
  ```python
  PORT = int(os.getenv('PORT', 8000))
  HOST = '0.0.0.0'
  ```

- **Line 27-46:** Excel file loading with error handling
  - Automatically looks for: `Open RO.xlsx`, `Open_RO.xlsx`, `open_ro.xlsx`
  - Gracefully handles missing files

### dashboard.html
- **Line 147:** Logo image path
  ```html
  <img id="companyLogo" src="./logo.jpg" alt="Company Logo" ...>
  ```

### requirements.txt
All dependencies for Render:
- fastapi - Web framework
- uvicorn - ASGI server
- pandas - Data processing
- openpyxl - Excel reading
- pydantic - Data validation

### render.yaml
Render configuration file - specifies:
- Service name
- Python version
- Build and start commands
- Environment variables

---

## ⚠️ Important Notes

### File Size Limitations
- Free tier Render has file system storage
- Excel file should be uploaded separately
- Consider using S3 for large files

### Excel File Location
The app looks for Excel file in the current working directory:

```python
for filename in ['Open RO.xlsx', 'Open_RO.xlsx', 'open_ro.xlsx']:
    if os.path.exists(filename):
        excel_file = filename
        break
```

### Logo File Location
Logo must be in the same directory as main.py:
- File: `logo.jpg`
- Path: Same as dashboard.html and main.py

### Port Configuration
Render automatically assigns a PORT. The code handles this:

```python
PORT = int(os.getenv('PORT', 8000))
```

---

## 🔧 Troubleshooting

### Issue: Excel file not found
**Solution:** Make sure Excel file is in the project root and named correctly:
- `Open RO.xlsx` (preferred)
- `Open_RO.xlsx`
- `open_ro.xlsx`

### Issue: Logo not displaying
**Solution:** Ensure:
1. Logo file is `logo.jpg`
2. Same folder as `main.py` and `dashboard.html`
3. JPG format (not PNG)

### Issue: Port binding error
**Solution:** The code handles this automatically:
```python
PORT = int(os.getenv('PORT', 8000))  # Render sets PORT env var
```

### Issue: Large Excel file causes slowdown
**Solution:** 
1. Optimize Excel file (remove extra sheets)
2. Or use AWS S3 for file storage
3. Implement caching

---

## 📊 API Endpoints

Once deployed, you can access:

```
GET  /                    → Dashboard HTML
GET  /api/dashboard/statistics
GET  /api/vehicles/mechanical
GET  /api/vehicles/bodyshop
GET  /api/vehicles/accessories
GET  /api/filter-options/{tab}
GET  /api/export/{tab}
GET  /health              → Health check
```

---

## 🚀 Deployment Comparison

| Feature | Local | Render |
|---------|-------|--------|
| URL | http://localhost:8000 | https://your-app.onrender.com |
| Port | 8000 | Automatic (env var) |
| Excel File | Local folder | Upload to service |
| Logo File | Local folder | Upload to service |
| Uptime | While running | 24/7 |
| Cost | Free (your computer) | Free tier available |

---

## 📝 Next Steps

1. ✅ Download all files from outputs
2. ✅ Create GitHub repository
3. ✅ Push code to GitHub
4. ✅ Connect Render to GitHub
5. ✅ Deploy on Render
6. ✅ Upload Excel and logo files
7. ✅ Access your dashboard online!

---

## 💡 Quick Reference

**Local Testing:**
```bash
python main.py
```
Then open: `http://localhost:8000`

**Render Deployment:**
```
1. GitHub → https://github.com
2. Render → https://render.com
3. Connect repository
4. Auto-deploy on push!
```

**File Requirements:**
```
main.py           ✅ Render-ready
dashboard.html    ✅ Render-ready
requirements.txt  ✅ Render-ready
render.yaml       ✅ Render config
Open RO.xlsx      ✅ Your data
logo.jpg          ✅ Your logo
```

---

## 🎉 Success Indicators

Once deployed, you should see:

1. ✅ Service running on Render
2. ✅ Dashboard accessible online
3. ✅ Excel data loading
4. ✅ Company logo displaying
5. ✅ All filters working
6. ✅ Export functionality available

---

## 📞 Need Help?

- **Render Docs:** https://render.com/docs
- **FastAPI Docs:** https://fastapi.tiangolo.com
- **GitHub Help:** https://docs.github.com

---

**Your Open RO Dashboard is now ready for Render deployment!** 🚀
