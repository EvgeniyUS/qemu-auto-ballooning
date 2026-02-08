Auto ballooning for QEMU guests (VMs) on a memory over-committed host.

The principle of operation is simple - the service is constantly trying to equalize the percentage of memory consumption between the node and the VM. 
By default, the acceptable range of values (the service does not change anything) is +-10% of the node's memory consumption. 
If the difference is greater, the service gives or takes away part of the VM's memory. 
Therefore, if more than 90% of the memory is used on the node, the node stops giving memory to the VMs, but it will take away almost all unused VMs memory.

The VirtIO Balloon Driver must be installed in the guest's operating system. It is pre-installed on most Linux systems.

Tested on Ubuntu 24.04 as host and Debian/Ubuntu as guests.