require 'json'
require 'nokogiri'
require 'ostruct'
module Embulk
  module Input
    module YahooAdsApi
      class ReportClient < Client
        def run(query)
          report_id = add_report(query)
          ::Embulk.logger.info "Create Report, report_id = #{report_id}"
          downloaded_report = report_download(report_id)
          ::Embulk.logger.info "Download Report, report_id = #{report_id}"
          data = xml_parse(downloaded_report)
          remove_report(report_id)
          ::Embulk.logger.info "Remove Report JOB, report_job_id = #{report_id}"
          data
        end

        private
        def add_report(config)
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
                  downloadFormat: "XML",
                  fields: config[:fields],
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
                  fields: config[:fields],
                  reportDateRangeType: config[:date_range_type],
                  reportDownloadEncode: "UTF-8",
                  reportDownloadFormat: "XML",
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

        def xml_parse(report)
          xml = Nokogiri::XML(report)
          columns = xml.css('column').map{|column| column.attribute('name').value }
          xml.css('row').map do |row|
            value = {}
            columns.each do |column|
              value[column.to_sym] = row.attribute(column).value
            end
            OpenStruct.new(value)
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
