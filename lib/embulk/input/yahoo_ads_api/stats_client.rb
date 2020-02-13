require 'json'
module Embulk
  module Input
    module YahooAdsApi
      class StatsClient < Client
        def initialize(servers, account_id, token)
          super(account_id, token)
          @base = servers + '/StatsService/'
        end

        def run(query)
          raw_data = []
          get_stats(query, raw_data)
          ::Embulk.logger.info "Get Stats"
          reshape_data(raw_data,query)
        end

        private
        def get_stats(config, raw_data, count=1)
          number_result = 1
          get_config = {
            accountId: @account_id,
            statsPeriod: config[:date_range_type],
            periodCustomDate: {
              statsEndDate: config[:end_date],
              statsStartDate: config[:start_date]
            },
            targetTypes: [
              "GENDER_TARGET"
            ],
            type: config[:stats_type],
            startIndex: (count-1)*number_result+1,
            numberResults: number_result
          }.to_json
          response = JSON.parse(self.invoke("get", get_config))
          if response.dig("rval","values").nil?
            raw_data
          else
            if response["rval"]["values"][0]["operationSucceeded"] == false
              error = response["rval"]["values"][0]["errors"]
              raise ::Embulk::Input::YahooAdsApi::Error::InvalidEnumError, error.to_json
            else
              raw_data.concat(response["rval"]["values"])
              if response["rval"]["totalNumEntries"] > number_result*count
                get_stats(config, raw_data, count+1)
              else
                raw_data
              end
            end
          end
        end

        def reshape_data(data,config)
          data_type = 'campaignStatsValue' if config[:stats_type] == "CAMPAIGN"
          data_type = 'adGroupStatsValue' if config[:stats_type] == "ADGROUP"
          data_type = 'adStatsValue' if config[:stats_type] == "AD"
          data.map do |value|
            stats_info = value[data_type]["stats"]
            value[data_type].delete("stats")
            stats_info.merge(value[data_type])
          end
        end
      end
    end
  end
end
