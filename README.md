# CS4459-A2
Part 1:
Run the backup server first so it can listen for requests from the primary.
Run with: python backup-part1.py portNumber
Any port number is okay, I tested 5001 and 9001 successfully.

Next run the primary with: python primary-part1.py portNumber
Any port number is okay as well. I tested 5001 and 9001 successfully.
Assignemnt said it was okay to assume the server name is localhost! 

Primary will automatically read "operations.txt" to perform actions. Then you can interact with it using a client afterwards.


Part 2:
Run all backups first. Ensure all backup port numbers are written into a "location.txt" file. 
CAREFUL: Assignment said to use "location.txt", not "locations.txt".
Primary will automatically read "operations.txt" to perform actions. Then you can interact with it using a client afterwards.

Note: backup-part2.py is identical to backup-part1.py as there were no neccessary changes for this assignment.
