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
          stats = get_stats(query)
          ::Embulk.logger.info "Get Stats"
          data = reshape_data(stats,query)
          data
        end

        private
        def get_stats(config)
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
            startIndex: 1,
            numberResults: 100
          }.to_json
          response = JSON.parse(self.invoke("get", get_config))
          if response["rval"]["values"][0]["operationSucceeded"] == false
            error = response["rval"]["values"][0]["errors"]
            raise ::Embulk::Input::YahooAdsApi::Error::InvalidEnumError, error.to_json 
          end
          response["rval"]["values"]
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
