require 'json'
require 'ostruct'
module Embulk
  module Input
    module YahooAdsApi
      class ReportClient < Client
        def initialize(servers, account_id, token)
          super(account_id, token)
          @base = servers + '/ReportDefinitionService/'
        end

        def run(query)
          report_id = add_report(query)
          ::Embulk.logger.info "Create Report, report_id = #{report_id}"
          data = CSV.parse(report_download(report_id).force_encoding("UTF-8"),headers: true)
          ::Embulk.logger.info "Download Report, report_id = #{report_id}"
          remove_report(report_id)
          ::Embulk.logger.info "Remove Report JOB, report_job_id = #{report_id}"
          data
        end

        def columns(type)
          params = yss? ? { reportType: type } : { type: 'AD' }
          response = self.invoke('getReportFields', params.to_json )
          columns = JSON.parse(response, symbolize_names: true)[:rval][:fields].map do |field|
            { request_name: field[:fieldName], api_name: field[:displayFieldNameJA] }
          end
          columns
        end

        def yss?
          @base.include? 'search'
        end

        private
        def add_report(config)
          column_name_list = config[:fields].map {|field| field["name"]}
          if config[:servers].include?("display")
            add_config = {
              accountId: @account_id,
              operand: [
                {
                  dateRange: {
                    endDate: config[:end_date],
                    startDate: config[:start_date]
                  },
                  dateRangeType: config[:date_range_type],
                  downloadEncode: "UTF-8",
                  downloadFormat: "CSV",
                  fields: column_name_list,
                  lang: "JA",
                  reportName: "YahooReport_#{DateTime.now.strftime("%Y%m%d_%H%I%s")}",
                }
              ]
            }.to_json
          elsif config[:servers].include?("search")
            add_config = {
              accountId: @account_id,
              operand: [
                {
                  dateRange: {
                      endDate: config[:end_date],
                      startDate: config[:start_date]
                  },
                  fields: column_name_list,
                  reportDateRangeType: config[:date_range_type],
                  reportDownloadEncode: "UTF-8",
                  reportDownloadFormat: "CSV",
                  reportLanguage: "JA",
                  reportName: "YahooReport_#{DateTime.now.strftime("%Y%m%d_%H%I%s")}",
                  reportType: config[:report_type],
                }
              ]
            }.to_json
          end
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
            reportJobId: report_job_id
          }.to_json
          sleep(wait_second)
          response = self.invoke('download', download_config)
          if response.start_with?("{") && response.end_with?("}")
            ::Embulk.logger.info "Waiting For Making Report"
            return report_download(report_job_id, wait_second * 2)
          else
            return response
          end
        end

        def remove_report(report_id)
          remove_config = {
            accountId: @account_id,
            operand: [
              {
                reportJobId: report_id,
              }
            ]
          }.to_json
          self.invoke('remove', remove_config)
        end
      end
    end
  end
end
