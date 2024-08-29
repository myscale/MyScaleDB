packer {
  required_plugins {
    amazon = {
      version = ">= 1.2.8"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

variables {
  ck_version_string = env("VERSION_STRING")
  myscale_version_major = env("MYSCALE_MAJOR")
  myscale_version_minor = env("MYSCALE_MINOR")
  myscale_version_patch = env("MYSCALE_PATCH")
}

locals {
  scripts_folder = "${path.root}/scripts"
  package_folder = "${path.root}/package"
  packer_folder = "${path.root}"
  upload_packer_folder = "${path.root}/upload"
  myscale_version_string = "${var.myscale_version_major}.${var.myscale_version_minor}.${var.myscale_version_patch}"
}
source "amazon-ebs" "myscale" {
  ami_name      = "myscale/images/myscaledb-${local.myscale_version_string}-amd64-{{timestamp}}"
  instance_type = "t2.medium"
  region        = "cn-northwest-1"
  source_ami_filter {
    filters = {
      name                = "myscale/images/*ubuntu-20_04-runtime-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["232980015707"]
  }
  launch_block_device_mappings {
    device_name = "/dev/sda1"
    volume_size = 40
    volume_type = "gp2"
    delete_on_termination = true
  }
  ssh_username = "ubuntu"
}

source "amazon-ebs" "ubuntu" {
  ami_name      = "myscale/images/ubuntu-20_04-runtime-amd64-aws-{{timestamp}}"
  instance_type = "t2.medium"
  region        = "cn-northwest-1"
  source_ami_filter {
    filters = {
      name                = "ubuntu/images/*ubuntu-focal-20.04-amd64-server-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["837727238323"]
  }
  ssh_username = "ubuntu"
}

build {
  name     = "myscale_runtime"
  sources  = [
    "source.amazon-ebs.ubuntu"
  ]
  provisioner "file" {
    source = "${local.scripts_folder}/init-runtime.sh"
    destination = "/tmp/init-runtime.sh"
  }
  provisioner "shell" {
    execute_command = "echo '' | sudo -S sh -c '{{ .Vars }} {{ .Path }}'"
    inline = ["echo 'Init Myscale Runtime'",
              "sudo bash /tmp/init-runtime.sh"]
  }
  provisioner "shell" {
  execute_command = "echo '' | sudo -S sh -c '{{ .Vars }} {{ .Path }}'"
  inline = ["sudo rm -f /home/ubuntu/.ssh/authorized_keys",
            "sudo rm -f /root/.ssh/authorized_keys"]
  }
}

build {
  name    = "myscale_db"
  sources = [
    "source.amazon-ebs.myscale"
  ]
  # upload file
  provisioner "file" {
    source = "${local.scripts_folder}/install-server.sh"
    destination = "/tmp/install-server.sh"
  }
  provisioner "file" {
    source = "${local.scripts_folder}/instance-env-check.sh"
    destination = "/tmp/instance-env-check.sh"
  }
  provisioner "file" {
    source = "${local.scripts_folder}/generate-dynamic-cfg.sh"
    destination = "/tmp/generate-dynamic-cfg.sh"
  }
  provisioner "file" {
    source = "${local.packer_folder}/upload.zip"
    destination = "/tmp/upload.zip"
  }
  provisioner "file" {
    source = "${local.scripts_folder}/cloud-init.yml"
    destination = "/tmp/cloud-init.yml"
  }
  provisioner "shell" {
    execute_command = "echo '' | sudo -S sh -c '{{ .Vars }} {{ .Path }}'"
    inline = ["sudo mv /tmp/cloud-init.yml /etc/cloud/cloud.cfg.d/99_custom.cfg"]
  }
  provisioner "shell" {
    execute_command = "echo '' | sudo -S sh -c '{{ .Vars }} {{ .Path }}'"
    inline = ["echo 'Init Myscale service AMI'",
              "sudo bash /tmp/install-server.sh ${var.ck_version_string} ${local.myscale_version_string}",
              "sudo mv /tmp/instance-env-check.sh /usr/local/bin/instance-env-check.sh",
              "sudo mv /tmp/generate-dynamic-cfg.sh /usr/local/bin/generate-dynamic-cfg.sh"]
  }
  provisioner "shell" {
    execute_command = "echo '' | sudo -S sh -c '{{ .Vars }} {{ .Path }}'"
    inline = ["sudo rm -f /home/ubuntu/.ssh/authorized_keys",
              "sudo rm -f /root/.ssh/authorized_keys"]
  }
}