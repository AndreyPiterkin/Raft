Project 6
Andrey Piterkin and Rohan Chotirmall

High-Level Approach:

We implemented the Raft protocol based on the Raft paper. 
It can handle put and get requests from clients, while maintaining two main goals: consistency and availability.
Through the Raft protocol and replicas, consistency is achieved, and the replicas are able to deal with other replicas 
failing, providing availability.

Challenges:

We faced a variety of challenges throughout this project. To deal with them, we had to look back to the Raft paper and
understand the protocol better.

To start, we had to handle elections. We were having trouble with this, and initially decided to deal with it by 
hardcoding the first election. We eventually were able to implement elections without hardcoding.

Another issue we faced was when we implemented AppendEntries RPCs with actual data, our replicas were constantly running
elections.

Design:

Our design follows the suggested design in the summary of the Raft paper. We think we followed it pretty well and wrote
our code in an 
easy-to-understand way. We broke down each aspect of the protocol into parts and minimized code duplication through a 
variety of methods.

Another good aspect of our design is the separation of socket management from the replicas. The SocketManager class 
deals with all the reading and writing to sockets

Testing:

We tested our code using the scripts provided, which gave us feedback on both the accuracy and performance of our 
implementation.

