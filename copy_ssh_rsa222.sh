#!/usr/bin/expect
set host [lindex $argv 0]
set username [lindex $argv 1]
set password [lindex $argv 2]

expect <<EOF
	spawn ssh-copy-id -i $username@$host
	set timeout 2
	expect {
	    "yes/no" { send "yes\n";exp_continue }
	    "password" { send "$password\n" }
	}
	expect "password" { send "$password\n" }
EOF

