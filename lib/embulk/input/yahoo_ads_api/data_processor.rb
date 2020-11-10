require 'csv'
require 'fileutils'

module Embulk
  module Input
    module YahooAdsApi
      class DataProcessor
        def initialize(task)
          @task = task
          token = Auth.new({
            client_id: task["client_id"],
            client_secret: task["client_secret"],
            refresh_token: task["refresh_token"]
          }).get_token
          @client = case @task["target"]
          when "report"
            ReportClient
          when "stats"
            StatsClient
          end.new(task["servers"], task["account_id"], token)
        end

        def run
          case @task['target']
          when "report"
            report_processor { |row| yield row }
          when "stats"
            stats_processor { |row| yield row }
          end
        end

        private
        def report_processor
          data = @client.run({
            servers: @task["servers"],
            date_range_type: 'CUSTOM_DATE',
            start_date: @task["start_date"],
            end_date: @task["end_date"],
            report_type: @task["report_type"],
            fields: @task['columns']
          })
          casts = @task['columns'].group_by { |c| c["type"] }.map do |k, v|
            case k
            when "timestamp"
              values = v.map { |c| [@task["columns"].index { |sc| sc == c }, c["format"]] }.compact
              [k, values]
            when "long", "double"
              values = v.map { |c| @task["columns"].index { |sc| sc == c } }.compact
              [k, values]
            else
              nil
            end
          end.compact.to_h
          File.open(data) do |file|
            file.each_line.with_index do |row, i|
              next if i == 0 || file.eof? # ignore first and last line
              processed_row = CSV.parse_line row.force_encoding("UTF-8")
              casts.each do |k, v|
                case k
                when "timestamp"
                  v.each { |i| processed_row[i[0]] = Time.strptime(row[i[0]], i[1]) }
                when "long"
                  v.each { |i| processed_row[i] = processed_row[i].to_i }
                when "double"
                  v.each { |i| processed_row[i] = processed_row[i].to_f }
                end
              end
              yield processed_row
            end
          end
          FileUtils.rm_r(File.dirname(data), :secure => true)
        end

        def stats_processor
          data = @client.run({
            servers: @task["servers"],
            date_range_type: 'CUSTOM_DATE',
            start_date: @task["start_date"],
            end_date: @task["end_date"],
            stats_type: @task["report_type"],
          })
          task_column_names = @task['columns'].map{|c| c["name"]}
          columns_list = @client.columns(@task["report_type"]).map { |c|
            [c[:request_name].to_sym, c[:api_name]] if task_column_names.include? c[:request_name]
          }.compact.to_h
          data.each_slice(100) do |rows|
            rows.each do |row|
              processed_row = @task['columns'].map do|column|
                col = columns_list[column["name"].to_sym]
                next if column.empty? || column.nil?
                if column["type"] == "timestamp"
                  Time.strptime(row[col],column["format"])
                elsif column["type"] == "long"
                  row[col]&.to_i
                elsif column["type"] == "double"
                  row[col]&.to_f
                else
                  row[col]
                end
              end
              yield processed_row
            end
          end
        end
      end
    end
  end
end
