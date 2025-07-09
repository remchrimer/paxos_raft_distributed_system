import java.io.*;
import java.util.*;
import java.net.*;
import java.util.regex.*;

public class Paxos {
    private final int port = 5050;
    private static Map<String, String> argMap = new HashMap<>();
    private static String value;
    private static int delay = 0;
    private static double proposal_num;
    private static boolean isProposer = false;
    private static List<String> hosts = new ArrayList<>();
    private static Map<Integer, Set<String>> proposers = new HashMap<>();
    private static Map<Integer, Set<String>> acceptors = new HashMap<>();
    private static Map<String, Set<String>> propose_accept_map = new HashMap<>();

    private String hostName;
    private Socket clientSocket;
    private DataInputStream input;
    private Map<String, DataOutputStream> list_output = new HashMap<>();

    private int received_peer_id;
    private String received_action;
    private String received_messageType;
    private String received_messageValue;
    private double received_proposalNum;

    private double min_proposer;
    private double accept_proposer;
    private String accept_value = "";
    private Map<Integer, List<Object>> prepare_majority_count = new HashMap<>();
    private Map<Integer, List<Object>> accept_majority_count = new HashMap<>();
    private boolean go_to_one = false;

    private void server(){
        try{
            ServerSocket serverSocket = new ServerSocket(port);
            while(true){
                clientSocket = serverSocket.accept();
                input = new DataInputStream(clientSocket.getInputStream());

                new Thread(() -> msgListener(clientSocket, input)).start();
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void msgListener(Socket clientSocket, DataInputStream input){
        try {
            String sender = clientSocket.getInetAddress().getHostName().split("\\.")[0];
            int senderIndex = hosts.indexOf(sender) + 1;
            while (true){
                String msg = input.readUTF();
                synchronized (this) {
                    if (isProposer) {
                        regex_msg_from(msg);
                        if (received_messageType.equals("prepare_ack")) {
                            handle_prepare_ack(senderIndex);
                            // Broadcast Accept(n, value) to all servers
                            broadcast("accept");
                        } else if (received_messageType.equals("accept_ack")) {
                            handle_accept_ack(senderIndex);
                        }
                    } else {
                        regex_msg_from(msg);
                        store_disk(received_peer_id, "received", received_messageType, received_messageValue, received_proposalNum);
                        if (received_messageType.equals("prepare")) {
                            respond_to_prepare();
                        } else if (received_messageType.equals("accept")) {
                            respond_to_accept();
                        }
                    }
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
            System.err.println("Error: " + e + "from msgListener function");
        }
    }

    private void connect() throws IOException {
        Set<String> peers = propose_accept_map.get(hostName);
        if(peers != null) {
            for (String h : peers) {
                Socket host_socket = new Socket(h, port);
                list_output.put(h, new DataOutputStream(host_socket.getOutputStream()));
            }
        }
    }

    private void sendMSG(String msg, String sendToAddr){
        try{
            if (list_output.containsKey(sendToAddr)) {
                list_output.get(sendToAddr).writeUTF(msg);
                list_output.get(sendToAddr).flush();
            }
        }
        catch(Exception e){
            System.err.println("Error: " + e + "from sendMSG function");
        }
    }

    private void broadcast(String message_type){
        int id = hosts.indexOf(hostName) + 1;
        String msg = store_disk(id, "sent", message_type, value, proposal_num);

        Set<String> peers = propose_accept_map.get(hostName);
        for(String p : peers){
            sendMSG(msg, p);
        }
    }

    private void handle_prepare_ack(int senderIndex){
        List<Object> values = new ArrayList<>();
        values.add(received_proposalNum);
        values.add(received_messageValue);
        prepare_majority_count.put(senderIndex, values);

        store_disk(received_peer_id, "received", received_messageType, received_messageValue, received_proposalNum);
        if (prepare_majority_count.size() > propose_accept_map.get(hostName).size() / 2){
            // If any acceptedValues returned, replace
            //value with acceptedValue for highest
            //acceptedProposal
            for (Map.Entry<Integer, List<Object>> entry : prepare_majority_count.entrySet()) {
                double proposal = (double) entry.getValue().get(0);
                String v = (String) entry.getValue().get(1);
                if (proposal > accept_proposer) {
                    value = v;
                }
            }
        }
    }

    private void handle_accept_ack(int senderIndex){
        // Any rejections (result > n)?
        // else value is chosen
        List<Object> values = new ArrayList<>();
        values.add(received_proposalNum);
        values.add(received_messageValue);
        accept_majority_count.put(senderIndex, values);

        store_disk(received_peer_id, "received", received_messageType, received_messageValue, received_proposalNum);
        if (accept_majority_count.size() > propose_accept_map.get(hostName).size() / 2){
            for (Map.Entry<Integer, List<Object>> entry : prepare_majority_count.entrySet()) {
                double proposal = (double) entry.getValue().get(0);
                String v = (String) entry.getValue().get(1);
                if (proposal > proposal_num) {
                    accept_majority_count = new HashMap<>();
                    prepare_majority_count = new HashMap<>();
                    proposal_num = 1 + proposal_num;
                    go_to_one = true;
                }
            }
            if(go_to_one){
                broadcast("prepare");
                go_to_one = false;
            }
            else{
                store_disk(hosts.indexOf(hostName) + 1, "chose", "chose", value, proposal_num);
            }
        }
    }

    private void respond_to_prepare() {
        if(received_proposalNum > min_proposer){
            min_proposer = received_proposalNum;
        }

        String msg = store_disk(hosts.indexOf(hostName) + 1, "sent", "prepare_ack", accept_value, accept_proposer);
        String proposer_peer_id = hosts.get(received_peer_id - 1);
        sendMSG(msg, proposer_peer_id);
    }

    private void respond_to_accept() {
        if(received_proposalNum >= min_proposer){
            accept_proposer = received_proposalNum;
            min_proposer = received_proposalNum;
            accept_value = received_messageValue;
        }

        String msg = store_disk(hosts.indexOf(hostName) + 1, "sent", "accept_ack", accept_value, min_proposer);
        String proposer_peer_id = hosts.get(received_peer_id - 1);
        sendMSG(msg, proposer_peer_id);
    }

    private static String store_disk(int peer_id, String action, String message_type, String message_value, double proposal_num){
        String msg = "{\"peer_id\": " + peer_id + ", \"action\": \"" + action + "\", \"message_type\": \""
                + message_type + "\", \"message_value\": \"" + message_value + "\", \"proposal_num\": " + proposal_num + "}";
        System.err.println(msg);
        return msg;
    }

    private static void readCommand(String[] args){
        for(int i = 0; i < args.length; i++){
            String curr = args[i];
            if (curr.equals("-h") || curr.equals("-v") || curr.equals("-t")) {
                if(i + 1 < args.length){
                    argMap.put(args[i], args[i + 1]);
                }
                else{
                    System.err.println("Command line arguments missing");
                    break;
                }
            }
        }
    }

    private static void readHostFile(String path){
        try (BufferedReader bf = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = bf.readLine()) != null){
                // split by : for hostnames
                // split by , for acceptors
                String[] split = line.split(":");
                String curr_peer_id = split[0];
                hosts.add(curr_peer_id);

                String[] roles = split[1].split(",");
                for (String r : roles) {
                    if (r.startsWith("proposer")) {
                        map_helper(proposers, r, curr_peer_id);
                    }
                    else if (r.startsWith("acceptor")) {
                        map_helper(acceptors, r, curr_peer_id);
                    }
                }
            }

            for(Map.Entry<Integer, Set<String>> e : proposers.entrySet()){
                String curr = e.getValue().iterator().next();
                propose_accept_map.putIfAbsent(curr, new HashSet<>());
                for (String a : acceptors.get(e.getKey())) {
                    propose_accept_map.get(curr).add(a);
                    propose_accept_map.putIfAbsent(a, new HashSet<>());
                    propose_accept_map.get(a).add(curr);
                }
            }
        }
        catch(Exception e){
            System.err.println("Error: " + e + "from readHostFile function");
        }
    }

    private static void map_helper(Map<Integer, Set<String>> map, String role, String peer_id){
        int id = regex_parse(role);
        map.putIfAbsent(id, new HashSet<>());
        map.get(id).add(peer_id);
    }

    private static int regex_parse(String s){
        Pattern pattern = Pattern.compile("(\\d+)$");
        Matcher matcher = pattern.matcher(s);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return 0;
    }

    private void regex_msg_from(String msg){
        Pattern pattern = Pattern.compile("\"(\\w+)\":\\s*(\"[^\"]*\"|\\d+(\\.\\d+)?)");
        Matcher matcher = pattern.matcher(msg);

        Map<String, String> map = new HashMap<>();
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2).replaceAll("\"", "");
            map.put(key, value);
        }

        received_peer_id = Integer.parseInt(map.get("peer_id"));
        received_action = map.get("action");
        received_messageType = map.get("message_type");
        received_messageValue = map.get("message_value");
        received_proposalNum = Double.parseDouble(map.get("proposal_num"));
    }

    public static void main(String[] args) throws IOException {
        try {
            Paxos p = new Paxos();

            readCommand(args);
            readHostFile(argMap.get("-h"));

            if (argMap.containsKey("-v")) {
                p.value = argMap.get("-v");
                isProposer = true;
            }
            if (argMap.containsKey("-t")) {
                p.delay = Integer.parseInt(argMap.get("-t"));
            }

            p.hostName = InetAddress.getLocalHost().getHostName().split("\\.")[0];
            proposal_num = 1 + (p.hosts.indexOf(p.hostName) + 1) * 0.1;

            new Thread(p::server).start();
            Thread.sleep(1000);
            p.connect();

            if(isProposer){
                // Broadcast Prepare(n) to all servers
                if(p.delay != 0){
                    Thread.sleep(p.delay * 1000);
                }
                p.prepare_majority_count = new HashMap<>();
                p.accept_majority_count = new HashMap<>();
                p.broadcast("prepare");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e + "from main function");
        }
    }
}

