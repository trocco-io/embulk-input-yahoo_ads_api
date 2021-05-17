require "json"

module Embulk
  module Input
    module YahooAdsApi
      class ReportClient < Client
        def initialize(ads_type, account_id, token)
          super(account_id, token)
          @base = SERVERS[ads_type.to_sym] + "/ReportDefinitionService/"
        end

        def run(query)
          report_id = add_report(query)
          ::Embulk.logger.info "Create Report, report_id = #{report_id}"
          file_path = report_download(report_id)
          ::Embulk.logger.info "Download Report, report_id = #{report_id}"
          remove_report(report_id)
          ::Embulk.logger.info "Remove Report JOB, report_job_id = #{report_id}"
          file_path
        end

        def columns(type)
          params = yss? ? { reportType: type } : { type: "AD" }
          response = self.invoke("getReportFields", params.to_json)
          columns = JSON.parse(response, symbolize_names: true)[:rval][:fields].map do |field|
            { request_name: field[:fieldName], api_name: field[:displayFieldNameJA] }
          end
          columns
        end

        def yss?
          @base.include? "search"
        end

        private

        def add_report(config)
          column_name_list = config[:fields].map { |field| field["name"] }
          add_config = {
            accountId: @account_id,
            operand: [
              {
                dateRange: {
                  endDate: config[:end_date],
                  startDate: config[:start_date],
                },
                fields: column_name_list,
                reportDateRangeType: config[:date_range_type],
                reportDownloadEncode: "UTF8",
                reportDownloadFormat: "CSV",
                reportLanguage: "JA",
                reportName: "YahooReport_#{DateTime.now.strftime("%Y%m%d_%H%I%s")}",
                reportType: config[:ads_type] == "yss" ? config[:report_type] : nil,
              }.reject { |_k, v| v.nil? },
            ],
          }.to_json
          response = JSON.parse(self.invoke("add", add_config))
          if response["rval"]["values"][0]["operationSucceeded"] == false
            error = response["rval"]["values"][0]["errors"]
            raise ::Embulk::Input::YahooAdsApi::Error::InvalidEnumError, error.to_json
          end
          response["rval"]["values"][0]["reportDefinition"]["reportJobId"].to_s
        end

        def report_download(report_job_id, wait_second = 5)
          download_config = {
            accountId: @account_id,
            reportJobId: report_job_id,
          }.to_json
          sleep(wait_second)
          file_path = self.temporarily_download("download", download_config)
          if json_response?(file_path)
            ::Embulk.logger.info "Waiting For Making Report (wait_second: #{wait_second})"
            File.open(file_path) do |file|
              file.each_line do |line|
                ::Embulk.logger.info line.chomp
              end
            end
            return report_download(report_job_id, wait_second * 2)
          else
            return file_path
          end
        end

        def remove_report(report_id)
          remove_config = {
            accountId: @account_id,
            operand: [
              {
                reportJobId: report_id,
              },
            ],
          }.to_json
          self.invoke("remove", remove_config)
        end

        def json_response?(file_path)
          left_flag = false; right_flag = false
          File.open(file_path) do |file|
            file.each_line.with_index do |line, i|
              left_flag = true if i == 0 && line.start_with?("{")
              right_flag = true if file.eof? && line.end_with?("}")
            end
          end
          return left_flag && right_flag
        end
      end
    end
  end
end
