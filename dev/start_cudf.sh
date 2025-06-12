#!/bin/bash
# Execute this script in host machine
#!/bin/bash
# Execute this script in host machine
set -eu

wget https://developer.download.nvidia.com/compute/cuda/12.9.1/local_installers/cuda-repo-rhel9-12-9-local-12.9.1_575.57.08-1.x86_64.rpm
sudo rpm -i cuda-repo-rhel9-12-9-local-12.9.1_575.57.08-1.x86_64.rpm
sudo dnf clean all
sudo dnf -y install cuda-toolkit-12-9

curl -s -L https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo | \
  sudo tee /etc/yum.repos.d/nvidia-container-toolkit.repo
sudo dnf install -y nvidia-container-toolkit

sudo dnf install -y epel-release
sudo dnf update -y
sudo dnf install -y dkms
sudo dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel9/x86_64/cuda-rhel9.repo
sudo dnf install -y cuda nvidia-driver
sudo dkms status
sudo reboot

# after reboot
#sudo dkms build nvidia/575.57.08 && sudo dkms install nvidia/575.57.08
#sudo nvidia-ctk runtime configure --runtime=docker
#sudo systemctl restart docker
#sudo docker run --rm --runtime=nvidia --gpus all ubuntu nvidia-smi
