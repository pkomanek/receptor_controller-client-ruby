require "receptor_controller/client/directive"

module ReceptorController
  class Client::DirectiveBlocking < Client::Directive
    def initialize(name:, account:, node_id:, payload:, client:)
      super
      self.response_lock = Mutex.new
      self.response_waiting = ConditionVariable.new
      self.response_data = nil
      self.response_exception = nil
    end

    def call(body = default_body)
      response = connection.post(config.job_path, body.to_json)

      msg_id = JSON.parse(response.body)['id']

      # registers message id for kafka responses
      response_worker.register_message(msg_id, self)
      wait_for_response(msg_id)
    # TODO: raise own errors
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Directive #{name} failed", account, node_id, e))
      raise
    end

    def wait_for_response(_msg_id)
      response_lock.synchronize do
        response_waiting.wait(response_lock)

        raise response_exception if response_failed?

        response_data.dup
      end
    end

    # TODO: More responses for one request are not used now
    def response_success(_msg_id, message_type, response)
      if message_type == MESSAGE_TYPE_RESPONSE
        self.response_data = response
      elsif message_type == MESSAGE_TYPE_EOF
        response_waiting.signal
      else
        self.response_exception = ReceptorController::Client::UnknownResponseTypeError
        response_waiting.signal
      end
    end

    def response_error(_msg_id, response_code, err_message)
      self.response_data = nil
      self.response_exception = ReceptorController::Client::ResponseError.new("#{err_message} (code: #{response_code})")
      response_waiting.signal
    end

    def response_timeout(_msg_id)
      self.response_data = nil
      self.response_exception = ReceptorController::Client::ResponseTimeoutError
      response_waiting.signal
    end

    private

    attr_accessor :response_data, :response_exception, :response_lock, :response_waiting

    def connection
      # https://github.com/lostisland/faraday/blob/master/lib/faraday/response/raise_error.rb
      # https://github.com/ansible/ansible_tower_client_ruby/blob/master/lib/ansible_tower_client/middleware/raise_tower_error.rb
      # Faraday::Response.register_middleware :raise_receptor_error => -> { Middleware::RaiseReceptorError }
      @connection ||= Faraday.new(config.controller_url, :headers => client.headers) do |c|
        c.use(Faraday::Response::RaiseError)
        c.adapter(Faraday.default_adapter)
        # c.response(:raise_receptor_error)
      end
    end

    def response_failed?
      response_exception.present?
    end
  end
end
