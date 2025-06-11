# MonSter

## About MonSter
MonSter is an “out-of-the-box” monitoring tool for high-performance computing platforms. It uses the evolving specification Redfish to retrieve sensor data from Baseboard Management Controller, and resource management tools such as Slurm to obtain application information and resource usage data. Additionally, it also uses a time-series database (TimeScaleDB implemented in the code) for data storage. MonSTer correlates applications to resource usage and reveals insightful knowledge without having additional overhead on the application and computing nodes. 

For details about MonSter, please refer to the paper: 
```
@inproceedings{li2020monster,
  title={MonSTer: an out-of-the-box monitoring tool for high performance computing systems},
  author={Li, Jie and Ali, Ghazanfar and Nguyen, Ngan and Hass, Jon and Sill, Alan and Dang, Tommy and Chen, Yong},
  booktitle={2020 IEEE International Conference on Cluster Computing (CLUSTER)},
  pages={119--129},
  year={2020},
  organization={IEEE}
}
```

For examples of visualization of data based on the above please see [https://idatavisualizationlab.github.io/HPCC/](https://idatavisualizationlab.github.io/HPCC/).

## Prerequisite
MonSter requires that iDRAC nodes (13G in pull model, 15G in push model), TimeScaleDB service, and Slurm REST API service can be accessed from the host machine where MonSter is running.

## Initial Setup

1. Copy the `config.yml.example` file to `config.yml` and edit the file to configure the iDRAC nodes, TimeScaleDB service, and Slurm REST API service.

2. The __usernames__ and __passwords__ should be configured in the environment (edit the `~/.bashrc` or `~/.bash_profile`) instead of hard-coded in the code or in the configuration file.

```bash
# For TimeScaleDB
tsdb_username=tsdb_username
tsdb_password=tsdb_password

# For iDRAC8
idrac_username=idrac_username
idrac_password=idrac_password

# For Slurm REST API
slurm_username=slurm_username
```

3. The database specified in the configuration file should be created and applied the TimeScaleDB extension before run any codes.

```bash
-- Create the database 'demo' for the owner 'monster',
CREATE DATABASE demo WITH OWNER monster;
-- Connect to the database
\c demo
-- Extend the database with TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

# MetricsBuilder

## About Metrics Builder

**Metrics Builder** acts as a middleware between the consumers (i.e. analytic clients or tools) and the producers (i.e. the databases). Its provides APIs for [the web applications](https://idatavisualizationlab.github.io/HPCC/) and accelerates the data query performance.

## Setup
Configure the SSL certificate and key for the MetricsBuilder API server. We use [Let's Encrypt](https://letsencrypt.org/) to get the SSL certificate and key.

```bash
export UVICORN_KEY=/path/to/ssl/key
export UVICORN_CERT=/path/to/ssl/cert
```

# Run MonSter and MetricsBuilder

1. Set up the virtual environment and install the required packages.

```bash
# Create the virtual environment
python3.9 -m venv .venv
# Activate the virtual environment
source .venv/bin/activate
# Install project in editable mode and install the required packages
pip install -e .
```

2. Copy config.yml.example and change the configuration accordingly.
```bash
# Copy config.yml.example and rename it
cp config.yml.example config.yml
``` 

3. Initialize the TimeScaleDB tables by running the `init_db.py` script.

```bash
python ./monster/init_tsdb.py --config=config.yml
```

4. Run the code to collect the data from iDRAC and Slurm.

```bash
nohup python ./monster/monit_idrac.py --config=config.yml >/dev/null 2>&1 &
nohup python ./monster/monit_slurm.py --config=config.yml >/dev/null 2>&1 &
```

5. Run the MetricsBuilder API server.

```bash
nohup python ./mbuilder/mb_run.py --config=config.yml >./log/mbapi.log 2>&1 &
```

6. Access the demo page of the MetricsBuilder API server at `https://localhost:5000/docs`.

7. Stop the running services.

```bash
kill $(ps aux | grep 'mb_run.py --config=config.yml' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'monit_idrac.py --config=config.yml' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'monit_slurm.py --config=config.yml' | grep -v grep | awk '{print $2}')
```

# Serving APIs with Nginx
## SSL Configuration (use hugo.hpcc.ttu.edu as an example)
### Prerequisites
- A valid DNS record for `hugo.hpcc.ttu.edu` pointing to the server’s public IP.
- `nginx` installed and running.
- `certbot` installed
### Step-by-Step SSL Setup
#### 1. Open Firewall Port 443
Allow HTTPS traffic through the firewall:
```bash
firewall-cmd --permanent --add-service=https
firewall-cmd --reload
```
To confirm it's active, try from you local computer:
```bash
nc -zv hugo.hpcc.ttu.edu 443
```
nc -zv hugo.hpcc.ttu.edu 443
#### 2. Ensure Nginx Has a Server Block for the Domain
Create or edit the Nginx configuration at `/etc/nginx/conf.d/hugo.hpcc.ttu.edu.conf`:
```nginx
server {
    listen 80;
    server_name hugo.hpcc.ttu.edu;

    root /usr/share/nginx/html;
    index index.html;
}
```
Open `/etc/nginx/nginx.conf` and make sure it includes the following line:
```nginx
include /etc/nginx/conf/*.conf;
```
Reload Nginx to apply changes:
```bash
nginx -t && systemctl reload nginx
```
#### 3. Issue the SSL Certificate with Certbot
Run the following Certbot command to automatically obtain and configure the certificate:
```bash
certbot --nginx -d hugo.hpcc.ttu.edu
```
This will:
- Obtain an SSL certificate from Let’s Encrypt.
- Modify the Nginx config to add a secure listen 443 ssl block.
- Configure automatic redirection from HTTP to HTTPS (if approved during prompts).

### Verification
- Visit `https://hugo.hpcc.ttu.edu` in your browser.
- Ensure the connection is secure and no *Not Secure* warnings appear.

## Serve APIs
#### Example URLs:
- `https://hugo.hpcc.ttu.edu/api/nocona/` -> FastAPI on port 5000
- `https://hugo.hpcc.ttu.edu/api/quanah/` -> FastAPI on port 5001
#### 1. Update Nginx Configuration
Edit the nginx configuration to include:
```nginx
server {
    listen 443 ssl;
    server_name hugo.hpcc.ttu.edu;
    ssl_certificate /etc/letsencrypt/live/hugo.hpcc.ttu.edu/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/hugo.hpcc.ttu.edu/privkey.pem; # managed by Certbot

    location /api/nocona/ {
        proxy_pass http://127.0.0.1:5000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /api/quanah/ {
        proxy_pass http://127.0.0.1:5001/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        root /usr/share/nginx/html;
        index index.html;
    }

    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;
}
```
#### 2. Update `root_path` in Fast API apps
**For /api/nocona/:**
```python
app = FastAPI(root_path="/api/nocona")
```
**For /api/quanah/:**
```python
app = FastAPI(root_path="/api/quanah")
```
> The partition name is defined in the configuration file; we set `root_path=f"/api/{partition}"` in the source code.
Then restart the service.
#### 3. Verification
Restart Nginx:
```bash
nginx -t && systemctl reload nginx
```
Access via:
- `https://hugo.hpcc.ttu.edu/api/nocona/docs` for the Nocona API
- `https://hugo.hpcc.ttu.edu/api/quanah/docs` for the Quanah API

#### 4. Trouble-shooting
If your system has SELinux enabled, it may block Nginx from making localhost connections.
Test with:
```bash
getenforce
```
If it says Enforcing, try (as root):
```bash
setenforce 0
```
Then reload Nginx and test again. If it works, you need a permanent SELinux policy:
```bash
sudo setsebool -P httpd_can_network_connect 1
```
This enables Nginx (`httpd`) to make outbound connections (like `proxy_pass`) permanently in SELinux policy.
