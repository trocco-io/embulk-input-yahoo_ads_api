module Embulk
  module Input
    module YahooAdsApi
      class Auth
        def initialize(options)
          @client_id = options[:client_id]
          @client_secret = options[:client_secret]
          @refresh_token = options[:refresh_token]
        end

        def get_token
          response = RestClient.get(
            'https://biz-oauth.yahoo.co.jp/oauth/v1/token?grant_type=refresh_token',
              {
                params:{
                  client_id: @client_id,
                  client_secret: @client_secret,
                  refresh_token: @refresh_token
                }
              })
          token = JSON.parse(response.body)["access_token"]
          return token
        end
      end
    end
  end
end
  