#!/usr/bin/expect
set timeout 5
set host [lindex $argv 0]
set username [lindex $argv 1]
set password [lindex $argv 2]

spawn ssh-copy-id -i $username@$host
	expect {
		"(yes/no)?"
		{
			send "yes\n"
			expect "*assword:" { send "$password\n"}
		}
		"*assword:"
		{
		send "$password\n"
		}
		"that are already installed

		" {
			#it has authorized, do nothing!
		}
	}
expect eof
