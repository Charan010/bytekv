package src.hashing;

public class Node{

private final String serverID;

public Node(String serverID){
    this.serverID = serverID;
}

public String getId(){
    return this.serverID;
}
}

