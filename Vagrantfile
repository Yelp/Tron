# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
# -*- ruby -*-
VAGRANTFILE_API_VERSION = "2"

stub_name = "tron-v"

trusty_box = 'puppetlabs/ubuntu-14.04-64-nocm'
precise_box = 'puppetlabs/ubuntu-12.04-64-nocm'
lucid_box = 'chef/ubuntu-10.04'

base_dir = File.dirname(__FILE__)
unless File.exists?("#{base_dir}/vagrant/insecure_tron_key")
  system("cd #{base_dir}/vagrant && ssh-keygen -q -t rsa -N '' -f insecure_tron_key")
end

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  nodes = [
    { name: 'batch-01', memory: '1024', box: lucid_box,  ip: '192.168.33.41' },
    { name: 'batch-02', memory: '1024', box: lucid_box,  ip: '192.168.33.42' },
    { name: 'batch-03', memory: '1024', box: trusty_box, ip: '192.168.33.43' },
  ]

  if Vagrant.has_plugin?("vagrant-cachier")
    config.cache.scope = :box
  end

  config.vm.define "master.#{stub_name}", primary: true do |master|

    master.vm.box = lucid_box
    master.vm.hostname = "master.#{stub_name}"

    master.vm.network :private_network, ip: "192.168.33.40"

    master.vm.provider "virtualbox" do |v|
      v.customize ["modifyvm", :id, "--memory", "1024"]
      v.name = "master.#{stub_name}"
    end


    # Development and operational package prereqs
    master.vm.provision :shell, inline: "apt-get update"
    master.vm.provision :shell, inline: "DEBIAN_FRONTEND=noninteractive apt-get install -y postfix"
    master.vm.provision :shell, inline: "apt-get install -y vim rsync python-twisted python-yaml python-dev python-pip python-virtualenv debhelper devscripts python-support cdbs python-central"
    if master.vm.box != lucid_box
      # Sphinx on Lucid does not support man page building
      master.vm.provision :shell, inline: "apt-get install -y python-sphinx"
    end
    master.vm.provision :shell, inline: "apt-get install -y python-daemon python-lockfile"
    master.vm.provision :shell, inline: "pip install pytz"

    # Dirty hack, as current package build is hardcoded to
    # /usr/local/bin/python
    master.vm.provision :shell, inline: "ln -sf /usr/bin/python /usr/local/bin/python"

    # Build the tron package
    master.vm.provision :shell, privileged: false, inline: "cd && rm -f tron_*.deb"
    master.vm.provision :shell, privileged: false, inline: "cd && rsync -r --exclude .vagrant --exclude .git /vagrant/ ~/tron/ && cd ~/tron && make deb"

    # pre-configure our playground trond
    master.vm.provision :shell, inline: "install -d -m 2750 -o vagrant -g vagrant /var/lib/tron"
    master.vm.provision :shell, inline: "install -d -m 2750 -o vagrant -g vagrant /var/log/tron"
    master.vm.provision :shell, inline: "cp /vagrant/vagrant/tron.default /etc/default/tron"
    master.vm.provision :shell, privileged: false, inline: "install -m 600 /vagrant/vagrant/insecure_tron_key /home/vagrant/.ssh/id_rsa"
    master.vm.provision :shell, inline: "install -m 644 /vagrant/vagrant/hosts /etc/hosts"

    # Fire up the requisite ssh-agent and load our private key.
    master.vm.provision :shell, privileged: false, inline: "ssh-agent > /var/lib/tron/ssh-agent.sh"
    master.vm.provision :shell, privileged: false, inline: ". /var/lib/tron/ssh-agent.sh && ssh-add /home/vagrant/.ssh/id_rsa"

    # and then install the package, ensuring stopped+started.
    master.vm.provision :shell, privileged: false, inline: "killall -9 trond >/dev/null && sleep 1 || true"
    master.vm.provision :shell, privileged: false, inline: "rm -f /var/lib/tron/tron.pid"
    master.vm.provision :shell, inline: "DEBIAN_FRONTEND=noninteractive dpkg --force-confold -i /home/vagrant/tron_*deb"
    master.vm.provision :shell, privileged: false, inline: ". /var/lib/tron/ssh-agent.sh && /usr/bin/trond -H 0.0.0.0"

  end

  nodes.each do |node|
    config.vm.define "#{node[:name]}.#{stub_name}" do |vm_config|

      vm_config.vm.box = node[:box]
      vm_config.vm.hostname = "#{node[:name]}.#{stub_name}"

      vm_config.vm.network :private_network, ip: node[:ip]
      node.fetch(:code_repos, []).each do |code_repo|
        vm_config.vm.synced_folder "../#{code_repo[:repo]}", "/srv/vagrant_repos/#{code_repo[:mount]}"
      end

      vm_config.vm.provider "virtualbox" do |v|
        v.customize ["modifyvm", :id, "--memory", node[:memory]]
        v.name = node[:name]
      end

      vm_config.vm.provision :shell, privileged: false, inline: "cat /vagrant/vagrant/insecure_tron_key.pub >> /home/vagrant/.ssh/authorized_keys"
      vm_config.vm.provision :shell, inline: "install -m 644 /vagrant/vagrant/hosts /etc/hosts"
      vm_config.vm.provision :shell, inline: "install -m 644 /vagrant/vagrant/sshd_config /etc/ssh/sshd_config"
      vm_config.vm.provision :shell, inline: "/etc/init.d/ssh reload"

    end

  end

end
