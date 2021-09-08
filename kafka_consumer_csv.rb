require "kafka"
require "json"
require "csv"

kafka = Kafka.new(["debian10:9092"])
consumer = kafka.consumer(group_id: "my-consumer-csv")
consumer.subscribe("quickstart-events", start_from_beginning: true)
trap("EXIT") { consumer.stop }

#デフォルトエンコーディング "utf-8"
fullload = CSV.open('C:\temp\fullload.csv','a')
changes = CSV.open('C:\temp\changes.csv','a')

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

  #CSVファイルをクリア
  if last_operation != "REFRESH" && operation == "REFRESH" then
    puts "Received RESET operation!!"
    fullload.truncate(0)
    fullload.puts ["EmployeeID","LastName","FirstName"]
    fullload.flush
    changes.truncate(0)
    changes.puts ["Operation","EmployeeID","LastName","FirstName"]
    changes.flush
  end

  #CSVファイルに追記
  if operation == "REFRESH" then
    fullload.puts [employeeid,lastname,firstname]
    fullload.flush
  elsif operation == "INSERT" || operation == "UPDATE" || operation == "DELETE" then
    changes.puts [operation,employeeid,lastname,firstname]
    changes.flush
  end
  last_operation = operation

  #set the checkpoint and commit
  consumer.mark_message_as_processed(message)
  consumer.commit_offsets
end

fullload.close
changes.close
