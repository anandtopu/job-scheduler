# Windows Setup Guide - Job Scheduler Project

## Issue Summary

Your project had multiple issues:

1. **Cassandra-driver build failure on Windows** - Due to Python 3.11+ and incompatible old setuptools
2. **Winget permission issues** - Not suitable for development tools
3. **Python version compatibility** - You wanted to upgrade from 3.11
4. **Pylint errors** - Code quality issues that needed resolution

This guide addresses all of them.

---

## Solution 1: Fix Cassandra-Driver on Windows

### Option A: Use Pre-Built Wheels (RECOMMENDED)

The issue is that cassandra-driver 3.29 tries to build from source and fails on Windows.

#### Step 1: Install from Pre-Built Wheel

```powershell
# Clear pip cache and install specific version
pip cache purge
pip install cassandra-driver==3.29.3 --only-binary :all: --no-cache-dir
```

If that fails, try using a binary repository:

```powershell
# Use Unofficial Windows Binaries for Python Extension Packages
pip install --upgrade --index-url https://files.pythonhosted.org/packages/ cassandra-driver==3.29.3
```

#### Step 2: Verify Installation

```powershell
python -c "from cassandra.cluster import Cluster; print('✓ Cassandra driver installed')"
```

---

### Option B: Avoid Building from Source (Docker Recommended)

Since cassandra-driver has build issues on Windows, **use Docker for development**:

```powershell
# Start only Cassandra and Redis in Docker
docker-compose up -d cassandra redis

# Your Python app connects to: cassandra:9042 → localhost:9042
# Run your Python app locally (requirements don't include cassandra-driver build)
pip install -e .
python -m src.api.app
```

---

## Solution 2: Python Version Upgrade (3.11 → 3.13)

### Changes Already Made ✅

- **Dockerfiles Updated**: All 3 Dockerfiles now use `python:3.13-slim`
- **GitHub Actions Updated**: Workflow now tests Python 3.11, 3.12, 3.13
- **Requirements Updated**: `setuptools>=68.0.0` and `wheel>=0.41.0` added

### Local Setup with Python 3.13

#### On Windows: Direct Installation (Recommended)

1. Download from: https://www.python.org/downloads/release/python-3130/
2. Run installer as Administrator
3. **Important**: Check ✓ "Add Python to PATH"
4. Verify:
   ```powershell
   python --version  # Should show 3.13.x
   ```

#### Alternative: Using Chocolatey (Recommended)

```powershell
# Install Chocolatey first (one-time)
Set-ExecutionPolicy Bypass -Scope Process -Force; 
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; 
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install Python 3.13
choco install python --version=3.13.0 -y

# Refresh PATH
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")

# Verify
python --version
```

#### Alternative: Using WSL (Windows Subsystem for Linux)

For better compatibility with Linux-based tools:

```powershell
# Enable WSL2
wsl --install

# In WSL terminal:
sudo apt-get update
sudo apt-get install python3.13 python3.13-venv python3-pip -y
```

---

## Solution 3: Winget Alternatives for Windows

### Don't Use Winget For:
- 🚫 Development tools (permission issues)
- 🚫 Python installations (use official installers)
- 🚫 Anything with admin requirements

### Better Alternatives:

| Tool | Use Case | Command |
|------|----------|---------|
| **Chocolatey** | Package manager (like apt-get) | `choco install python` |
| **Direct Download** | Python official releases | https://python.org |
| **Scoop** | User-mode package manager | `scoop install python` |
| **Windows Store** | Some apps (Microsoft Python) | Microsoft Store App |

---

## Solution 4: Pylint Issues Fixed ✅

### Errors Resolved

1. **W0611: Unused import json** - Removed from test_worker.py
2. **W0212: Protected member access** - Added pragma comments for tests
3. **R0801: Duplicate code** - Extracted to `_make_empty_result()` helper

### Configuration Added

Created `.pylintrc` with reasonable defaults for Python projects:
- Allows protected access in tests
- Handles redefined outer names (pytest fixtures)
- Allows flexible naming conventions

---

## Complete Setup Steps

### Step 1: Install Python 3.13

**Using Direct Installer (Easiest):**
- Download: https://www.python.org/downloads/
- Run installer
- Check "Add Python to PATH"
- Verify: `python --version`

**OR Using Chocolatey:**
```powershell
choco install python --version=3.13.0 -y
```

### Step 2: Clone & Setup Virtual Environment

```powershell
cd C:\Users\anand\Downloads\Learning\SystemDesign\job-scheduler

# Create virtual environment with Python 3.13
python -m venv venv

# Activate
venv\Scripts\activate

# Upgrade pip, setuptools, wheel
python -m pip install --upgrade pip setuptools wheel

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Step 3: Test Installation

```powershell
# Verify all imports work
python -c "
from cassandra.cluster import Cluster
import redis
import fastapi
print('✓ All dependencies installed successfully!')
"

# Run linting
pylint src/

# Run tests
pytest tests/
```

### Step 4: Build Docker Images (for production)

```powershell
docker-compose build
docker-compose up
```

---

## Troubleshooting

### Issue: "cassandra-driver Failed to build"

**Solution:**
```powershell
# Method 1: Pre-built wheel
pip install cassandra-driver==3.29.3 --only-binary :all:

# Method 2: Use Docker
docker-compose up -d cassandra redis
# Then don't install cassandra-driver locally
```

### Issue: "Python not found" or "python --version" shows wrong version

**Solution:**
```powershell
# Check which Python is active
Get-Command python | Select-Object Source

# If wrong version, specify full path
C:\Python313\python.exe --version

# Add to PATH manually:
$env:Path += ";C:\Python313"
```

### Issue: "ModuleNotFoundError: No module named 'cassandra'"

**Solutions (in order):**
1. Recreate venv: `python -m venv venv --clear; venv\Scripts\activate`
2. Reinstall: `pip install --force-reinstall cassandra-driver==3.29.3`
3. Use Docker Compose instead for Cassandra

### Issue: Permission denied when installing with pip

**Solution:**
```powershell
# Ensure venv is activated
venv\Scripts\activate

# Install with --user flag if needed
pip install --user cassandra-driver

# Or run as admin (not recommended):
# Right-click PowerShell → Run as Administrator
```

---

## Next Steps

### 1. Verify Everything Works
```powershell
# Run tests
pytest tests/ -v

# Run linting
pylint src/ tests/

# Check code coverage
pytest tests/ --cov=src
```

### 2. Commit Changes to GitHub

The following files have been updated:

```
✅ docker/api.Dockerfile
✅ docker/worker.Dockerfile
✅ docker/scheduler.Dockerfile
✅ .github/workflows/pylint.yml
✅ requirements.txt
✅ tests/conftest.py
✅ tests/unit/test_scheduler.py
✅ tests/unit/test_worker.py
✅ tests/unit/test_queue.py
✅ .pylintrc (new)
```

### 3. Deploy

```powershell
# Build and push Docker images
docker-compose build
docker-compose push

# Or deploy to Kubernetes
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/
```

---

## Python Version Compatibility Matrix

| Version | Status | Windows | Docker | Comment |
|---------|--------|---------|--------|---------|
| 3.10 | Maintenance | ✅ | ✅ | Old, not recommended |
| 3.11 | Stable | ⚠️ Build issues | ✅ | Current in your project |
| 3.12 | Stable | ✅ | ✅ | Good compatibility |
| 3.13 | Latest | ✅ | ✅ | **RECOMMENDED** |
| 3.14 | Beta | ⚠️ | ⚠️ | Use only for testing |

---

## Files Modified Summary

### Requirements
- Added `setuptools>=68.0.0` and `wheel>=0.41.0` for proper package building
- Updated cassandra-driver requirement comment

### Code Quality
- Created `.pylintrc` for consistent linting
- Fixed all pylint errors (W0611, W0212, R0801)
- Extracted duplicate mock setup to helper function

### CI/CD
- Updated GitHub Actions to test 3.11, 3.12, 3.13
- Added dependencies installation to CI workflow

### Docker
- Updated all Dockerfiles to use Python 3.13
- No runtime changes needed

---

## Quick Reference Commands

```powershell
# Setup
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Testing
pytest tests/ -v
pylint src/

# Docker
docker-compose up
docker-compose logs -f api

# Kubernetes (optional)
kubectl apply -f k8s/
kubectl get pods -n job-scheduler

# Cleanup
deactivate
docker-compose down -v
```

---

## Support

For issues specific to Windows Python development, visit:
- https://docs.python.org/3/using/windows.html
- https://github.com/pypa/pip/issues
- https://github.com/apache/cassandra-python-driver/issues

