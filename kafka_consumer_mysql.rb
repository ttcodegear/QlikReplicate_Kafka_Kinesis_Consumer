require "kafka"
require "json"
require 'mysql2'

client = Mysql2::Client.new(:host => 'localhost', :username => 'test', :password => 'test', :database => 'testdb', :encoding => 'utf8mb4', :reconnect => true)

kafka = Kafka.new(["debian10:9092"])
consumer = kafka.consumer(group_id: "my-consumer-mysql")
consumer.subscribe("quickstart-events", start_from_beginning: true)
trap("EXIT") { consumer.stop }

last_operation = "REFRESH"

#Messages will not be marked as processed automatically
consumer.each_message(automatically_mark_as_processed: false) do |message|
  #コンソールに出力
  #puts message.offset, message.key, message.value

  #parse JSON to object
  cdcdata = JSON.parse(message.value, object_class: OpenStruct)
  operation = cdcdata.message.headers.operation
  employeeid = cdcdata.message.data.EmployeeID
  lastname = cdcdata.message.data.LastName
  firstname = cdcdata.message.data.FirstName
  #コンソールに出力
  puts operation, employeeid, lastname, firstname

  #MySQLのテーブルをクリア
  if last_operation != "REFRESH" && operation == "REFRESH" then
    puts "Received RESET operation!!"
    client.query("DELETE FROM fullload")
    client.query("DELETE FROM changes")
  end

  #MySQLにINSERT
  if operation == "REFRESH" then
    statement = client.prepare('INSERT INTO fullload(EmployeeID, LastName, FirstName) values(?, ?, ?)')
    begin
      statement.execute(employeeid, lastname, firstname)
      #puts client.affected_rows
    rescue Exception => e
      puts e.message
    end
  elsif operation == "INSERT" || operation == "UPDATE" || operation == "DELETE" then
    statement = client.prepare('INSERT INTO changes(Operation, EmployeeID, LastName, FirstName) values(?, ?, ?, ?)')
    statement.execute(operation, employeeid, lastname, firstname)
    #puts client.affected_rows
  end
  last_operation = operation

  #set the checkpoint and commit
  consumer.mark_message_as_processed(message)
  consumer.commit_offsets
end
