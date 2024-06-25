# Tools Usage

## Install Packer

```bash
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install packer
```

## Build AMI runtime:

```bash
export AWS_ACCESS_KEY_ID="<aws-access-key>"
export AWS_SECRET_ACCESS_KEY="<aws-access-secret>"

packer build -only 'myscale_runtime.*' ./aws-ubuntu_20_04.pkr.hcl
```

## Build AMI:

```bash
bash scripts/prepare.sh
source scripts/ami-version.sh

export AWS_ACCESS_KEY_ID="<aws-access-key>"
export AWS_SECRET_ACCESS_KEY="<aws-access-secret>"

packer build -only 'myscale_db.*' ./aws-ubuntu_20_04.pkr.hcl
```

