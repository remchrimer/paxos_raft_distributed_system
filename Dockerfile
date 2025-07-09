FROM openjdk:17

ADD Paxos.java /app/
ADD hostsfile-testcase1.txt /app/
ADD hostsfile-testcase2.txt /app/
WORKDIR /app/
RUN javac Paxos.java

ENTRYPOINT ["java", "Paxos"]
