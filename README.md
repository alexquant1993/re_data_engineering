# Step 1: Create a new project in Google Cloud Platform
- Create a Google Cloud Platform account
- Create a new project: idealista-scraper. ID = idealista-scraper-384619
- Enable Compute Engine API

# Step 2: Install Google Cloud SDK
- Install Google Cloud SDK from https://cloud.google.com/sdk/docs/install#windows
    - On Windows use chocolatey, make sure you run CMD on admin mode: `choco install google-cloud-sdk`
    - To update: `choco upgrade google-cloud-sdk`
- Check installation with `gcloud version`. Output:
```bash
Google Cloud SDK 427.0.0
bq 2.0.91
core 2023.04.17
gcloud-crc32c 1.0.0
gsutil 5.23
```

# Step 3: Configure permissions with Google Cloud CLI
Create an IAM service account to enable Terraform access to Google Cloud Platform.
- Login to Google Cloud Platform with `gcloud auth login`
- Update GCP: `gcloud components update`
- Create a variable with the project ID:`PROJECT_ID="idealista-scraper-384619"`
- Set to project `gcloud config set project $PROJECT_ID`
- Create a service account: `gcloud iam service-accounts create terraform-iam --display-name "terraform-iam"`
- Check service account: `gcloud iam service-accounts list`
- Define roles:

```bash
# This role provides full access to resources within the project, including the ability to create and delete resources.
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/editor"
# This role provides full access to Google Cloud Storage resources within the project, including the ability to create and delete buckets and objects.
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"
# This role provides full access to objects within Google Cloud Storage buckets within the project, including the ability to create and delete objects.
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
# This role provides full access to BigQuery resources within the project, including the ability to create and delete datasets and tables.
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:terraform-iam@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"
```
- Create directory to store credentials: `mkdir ~/.gcp`
- Download JSON credentials: `gcloud iam service-accounts keys create ~/.gcp/terraform.json --iam-account=terraform-iam@$PROJECT_ID.iam.gserviceaccount.com`
- Login with service account: `gcloud auth activate-service-account --key-file ~/.gcp/terraform.json`

# Step 4: Install Terraform
Terraform is a tool for infrastructure as code, allowing you to define and manage your infrastructure in a declarative manner. With Terraform, you can provision and manage infrastructure across multiple providers and environments, making it easier to maintain consistency and reduce errors. Terraform also provides a way to version and track changes to your infrastructure, making it easier to collaborate and audit changes over time.

- Install Terraform: https://www.terraform.io/downloads.html
    - On Windows use chocolatey, make sure you run CMD on admin mode: `choco install terraform`
    - To update: `choco upgrade terraform`
- Check that Terraform is installed: `terraform version`. Output:
```bash
Terraform v1.4.5
on windows_amd64
```

# Step 5: Create GCP resources with Terraform
- Create terraform directory: `mkdir terraform`
- Move to terraform directory: `cd terraform`
> We will sign up for the EC2 instance savings plan for a year to get a discount on the VMs.
- Create a file named `main.tf` where we will define the resources we want to create in AWS.
    - Create two VM instance: t4g.small (2 vCPUs, 4 GB memory), Ubuntu 22.04 LTS, 20 GB disk. A ssh key pair will be used to connect to the VM. NOTE: Some regions are disabled by default, for instance eu-south-2 (Spain) was disabled in my case. To enable it, go to IAM > Account Settings > Region and select the region you want to enable.
    - Create a GCS bucket
    - Create a BigQuery dataset
- Create a file named `variables.tf` where we will define the variables we will use in `main.tf`.
    - Establish the project ID
    - Establish the region and zone
    - Establish VM details: machine type, image, disk size
    - Establish GCS bucket name and storage class
    - Establish BigQuery dataset name
- Initialize Terraform: `terraform init`
- Terraform validate: `terraform validate`
- Terraform plan: `terraform plan -var "credentials=~/.gcp/terraform.json" -var "vm_ssh_user=aarroyo" -var "vm_ssh_pub_key=~/.ssh/idealista_vm.pub" -out=tfplan`
    - Enter DigitalOcean token - DO is used given that offers cheaper VMs than GCP. The token can be obtained from the DO dashboard.
    - Enter GCP credentials JSON file path: `~/.gcp/terraform.json`
    - Enter path to the SSH public key for VM: `~/.ssh/idealista_vm.pub`
        - Previously generate a SSH key pair: `ssh-keygen`
        - Select a file name to save the key pair: `~/.ssh/idealista_vm`
    - Enter SSH username for VM: `aarroyo`
- Terraform apply: `terraform apply "tfplan"`
- Check that the resources have been created in GCP dashboard.
- If you want to destroy the resources created with Terraform: `terraform destroy -var "credentials=~/.gcp/terraform.json" -var "vm_ssh_user=aarroyo" -var "vm_ssh_pub_key=~/.ssh/idealista_vm.pub"`

# Step 6: Connect to VM instance
- Install Remote SSH extension in VSCode
- Check the VM instance IP address in the DO dashboard
- Edit the config file in `~/.ssh/config` to add the VM instance:
```bash
Host idealista_vm_{type_pipeline}
	Hostname {vm_ip_address}
	User ubuntu
    Port 22
	IdentityFile {~/.ssh/private_ssh_key}
```
- Connect to VM instance using the Remote SSH extension in VSCode. Select remote and then connect in new window.
- This will open a new VSCode window with the VM instance connected.
- Create a new user in the VM instance: `sudo adduser aarroyo && sudo usermod -aG sudo aarroyo`
- Change user to `aarroyo`: `sudo su - aarroyo`
- Add the SSH public key to the user. [Check AWS tutorial](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/managing-users.html)
```bash
mkdir .ssh
chmod 700 .ssh
touch .ssh/authorized_keys
chmod 600 .ssh/authorized_keys
nano .ssh/authorized_keys
# Paste public key idealista_vm.pub here and save.
```
- Now you will be able to connect to the VM instance using the `aarroyo` user. Change the config file accordingly.

# Step 7: Install packages and software in VM instance
- Check CPU and disk space information: `lscpu` and `df -h`
- Install and update packages:
```bash
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install bzip2 libxml2-dev
# Install the required language pack
sudo apt-get install language-pack-es-base
```

> The anaconda URL is for ARM64 architecture. If you are using a different architecture, check the [Anaconda website](https://www.anaconda.com/products/individual) to get the correct URL.

- Install Anaconda:
```bash
wget https://repo.anaconda.com/archive/Anaconda3-2023.09-0-Linux-aarch64.sh -O ~/anaconda.sh
bash ~/anaconda.sh -b -p $HOME/anaconda3
echo 'export PATH="$HOME/anaconda3/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
rm -f ~/anaconda.sh
```
> To fix the SSH warning after destroying and recreating a VM instance with Terraform, remove the old key fingerprint from the known_hosts file and try to connect again to the VM.

- Install and setup Git:
    - Install git: `sudo apt-get install git`
    - Configure git: `git config --global user.name "Your Name"` and `git config --global user.email youremail@example.com`
- Generate a SSH key: `ssh-keygen -t rsa -b 4096 -C "youremail@example.com"`
- Add the SSH key to your GitHub account: 
    - Copy the public SSH key: `cat ~/.ssh/id_rsa.pub`
    - Go to GitHub settings > SSH and GPG keys > New SSH key
    - Paste the public SSH key and save
    - Test the SSH connection: `ssh -T git@github.com`
- Clone the repository: `git clone git@github.com:alexquant1993/real_estate_spain.git`
- Create a conda environment and install the neccesary packages:
```bash
conda create -n re-spain python=3.11
conda init bash
# Close and reopen your terminal
conda activate re-spain
cd real_estate_spain
pip install -r requirements.txt
# Downgrade pydantic to make it compatible with prefect
pip install pydantic==1.10.11
```
- Set the PYTHONPATH environment variable to include our working directory, in the .bashrc file:
```bash
nano ~/.bashrc
export PYTHONPATH="${PYTHONPATH}:/home/aarroyo/real_estate_spain"
source ~/.bashrc
```

## Prefect setup
1. Run prefect as a (systemd service)[https://docs.prefect.io/orchestration/tutorial/overview.html#running-prefect-as-a-systemd-service].
    - Create a new systemd service unit file in the /etc/systemd/system/ directory: `sudo nano /etc/systemd/system/prefect-agent.service`
    - Copy and paste the following configuration into the service file. Replace {type_pipeline} by `sale`, `rent` or `share`:
    ```bash
    [Unit]
    Description=Prefect Agent

    [Service]
    Type=simple
    User=aarroyo
    WorkingDirectory=/home/aarroyo/real_estate_spain
    ExecStart=/home/aarroyo/anaconda3/envs/re-spain/bin/prefect agent start -q {type_deployment}
    Restart=always

    [Install]
    WantedBy=multi-user.target
    ```
    - Reload the systemd daemon to read the new service configuration: `sudo systemctl daemon-reload`
    - Enable the service to ensure that it starts on boot: `sudo systemctl enable prefect-agent`
    - Start the prefect service: `sudo systemctl start prefect-agent`
    - Check the status of the service: `sudo systemctl status prefect-agent`
    - Check that everything is working properly: `systemctl --type=service | grep prefect`

> If the agent is not picking up the flow runs you can do: `sudo systemctl daemon-reload && sudo systemctl restart prefect-agent && sudo systemctl status prefect-agent`

2. Repeat steps in section 3 in order to connect to Google Cloud with a service account:
    - In a Digital Ocean Droplet, you have to install the Google Cloud SDK: `sudo snap install google-cloud-sdk --classic`
    - Login to Google Cloud Platform with `gcloud auth login`
    - Export project ID value: `export PROJECT_ID="idealista-scraper-384619"`
    - Set project: `gcloud config set project $PROJECT_ID`
    - Export service account name: 
        - Sale VM: `export SERVICE_ACC_NAME="prefect-agent-sale"`
        - Rent VM: `export SERVICE_ACC_NAME="prefect-agent-rent"`
    - Create a service account: `gcloud iam service-accounts create $SERVICE_ACC_NAME --display-name "Prefect Agent"`
    - Add GCS and Bigquery roles to the service account:
        - GCS: 
            - `gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACC_NAME@$PROJECT_ID.iam.gserviceaccount.com --role roles/storage.admin`
            - `gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACC_NAME@$PROJECT_ID.iam.gserviceaccount.com --role roles/storage.objectAdmin`
        - Bigquery: `gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACC_NAME@$PROJECT_ID.iam.gserviceaccount.com --role roles/bigquery.admin`
    - Download JSON credentials: `gcloud iam service-accounts keys create ~/.gcp/prefect-agent.json --iam-account=$SERVICE_ACC_NAME@$PROJECT_ID.iam.gserviceaccount.com`
    - Login with service account: `gcloud auth activate-service-account --key-file ~/.gcp/prefect-agent.json`

# Step 8: Run the pipelines in VM instance
- Login to prefect cloud: `prefect cloud login -k {YOUR_API_KEY}`
- Create deployment file, for instance for the rent pipeline you will have:
```bash
export PIPELINE="rent"
prefect deployment build src/flows/idealista_flow.py:idealista_to_gcp_pipeline \
-n madrid_${PIPELINE}_daily \
-t rent \
-q rent \
-o prefect_pipelines/madrid_${PIPELINE}_daily.yaml
```
- Deployment file customization:
    - Set up parameters according to your needs:
        - testing: false
        - province: madrid
        - bucket_name: idealista_data_lake_idealista-scraper-384619
        - time_period: 24
        - type_search: rent
        - credentials_path: ~/.gcp/prefect-agent.json
    - Set up the schedule to run the pipeline every day at 7:00 AM:
        - cron: 0 7 * * *
        - timezone: Europe/Madrid
        - day_or: true
- Apply deployment: `prefect deployment apply prefect_pipelines/madrid_rent_daily.yaml`
- Run flow: `prefect deployment run "idealista-to-gcp-pipeline/madrid_rent_daily"`
- Another testing deployment can be created as well, following the steps above.
- Implement an automation feature in Prefect Cloud that enables the automatic dispatch of emails triggered by specific flow run events. These events should include when a flow run is completed, cancelled, or has failed.
> Disable HTTP2 for prefect, [to avoid httpx.LocalProtocolError](https://github.com/PrefectHQ/prefect/issues/7442): `prefect config set PREFECT_API_ENABLE_HTTP2=false`


# Scraping techniques

## Request headers
Headers are used to mimic a real browser request. In order to get the headers from your browser:
- Open your browser, then go to the Network tab in the Developer Tools and copy the request headers from the first request.
- Or execute the following Python code and then go to http://127.0.0.1:65432/ to check the headers:
```python
import socket

HOST = "127.0.0.1" 
PORT = 65432

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
        with conn:
            print(f"connected by {addr}")
            data = conn.recv(1024)
            print(data.decode())
            # header
            conn.send(b'HTTP/1.1 200 OK\n')
            conn.send(b'Content-Type: text/html\n')
            conn.send(b'\n')
            # body
            conn.send(b'<html><body><pre>')
            conn.send(data)
            conn.send(b'</pre></body></html>')
            conn.close()
```
Executing the above Python code, you will get the following headers using a Chrome browser:
```
GET / HTTP/1.1
Host: 127.0.0.1:65432
Connection: keep-alive
sec-ch-ua: "Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36
Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7
Sec-Fetch-Site: none
Sec-Fetch-Mode: navigate
Sec-Fetch-User: ?1
Sec-Fetch-Dest: document
Accept-Encoding: gzip, deflate, br
Accept-Language: es-ES,es;q=0.9,en;q=0.8
```
The headers order is important, so you have to keep the same order as they appear in a web browser.

### Accept
Indicates what type of content the browser accepts. The value is a list of media types, such as text/html, image/png, etc. The default value is */*.
```
# Firefox
text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8
# Windows
text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
```

### Accept-Encoding
Indicates what encoding schemes the browser supports. The value is a list of encoding types, such as gzip, deflate, etc.
```
# Firefox with brotli support
gzip, deflate, br
# Windows without brotli support
gzip, deflate
```
### Accept-Language
Indicates the user's preferred language. The value is a list of language types, such as en-US, en, etc. Keep it in the language of the country you are scraping.
```
# Windows
es-ES,es;q=0.9
```

### Upgrade-Insecure-Requests
Indicates whether the browser can upgrade from HTTP to HTTPS. The value is 1 if the browser can upgrade; otherwise, it's omitted.

### User-Agent
The User-Agent request header contains a characteristic string that allows the network protocol peers to identify the application type, operating system, software vendor, or software version of the requesting software user agent. For web scraping purposes, the most common available user agents are preferred. The user agent configuration should match other headers such as Accept, Accept-Encoding, Accept-Language, etc.
```
# Chrome on Windows
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36
# Firefox on Windows
Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0
```

### Sec-Ch family
The Sec-Ch family of headers are used to communicate browser security capabilities to the server. The Sec-Ch headers are used to communicate the browser's User-Agent Client Hints to the server. They should match the user agent configuration. Not all browsers support the Sec-Ch headers. They are experimental and subject to change.

## Sec-Fetch family
The Sec-Fetch headers are used to communicate the browser's navigational context to the server. They should match the user agent configuration.
- 'sec-fetch-site': indicates the origin of the request. 'none' for direct requests, 'same-site' for dynamic requests (XHR).
- 'sec-fetch-mode': indicates navigational mode. 'navigate' for direct requests, and 'same-origin', 'cors' or 'no-cors' for dynamic requests.
- 'sec-fetch-user': indicates whether requests was made by user or javascript. Alwasys '?1' or omitted.
- 'sec-fetch-dest': indicates requested document type. 'document' for direct HTML requests and empty for dynamic requests.

### Referer
The Referer request header contains the address of the previous web page from which a link to the currently requested page was followed. The Referer header allows servers to identify where people are visiting them from and may use that data for analytics, logging, or optimized caching, for example. The Referer header must match the origin of the request.

### Cookie
The Cookie header contains stored HTTP cookies previously sent by the server with the Set-Cookie header. The Cookie header must match the origin of the request. This is already managed by httpx library.

## Rate limiting
Rate limiting is a mechanism that controls the rate of incoming requests to a server in order to prevent overloading. The rate limiting is important to avoid being banned by the server. The rate limiting can be done by adding some delay between requests. In our case this has been set by 30 seconds between requests. This has been done by using the Token Bucket algorithm, which can be described as follows:
- A token is added to the bucket every 30 seconds.
- The bucket can hold a maximum of 1 token.
- If the bucket is full, the token is discarded.
- When a request is made, a token is removed from the bucket.
- If the bucket is empty, the request is delayed until a token is available.

## Randomization
Randomization is a mechanism that adds some randomness to the requests in order to avoid being detected as a bot. In our case, the following randomization techniques have been used:
- Random headers: a random set of headers is selected from the list of most popular OS and browsers every 30 requests.
    - Windows + Chrome
    - Windows + Firefox
    - Mac + Safari
    - Mac + Chrome
    - Mac + Firefox
- Random delays between requests: a random delay between 1 and 5 seconds is added between requests.
- Random initial delay before starting the scraping process: a random delay between 0-30 minutes.
- Random time after the scraping of pages is finished: a random delay between 1-3 hours.



