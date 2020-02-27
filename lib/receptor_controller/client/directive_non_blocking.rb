require "receptor_controller/client/directive"

module ReceptorController
  class Client::DirectiveNonBlocking < Client::Directive
    def initialize(name:, account:, node_id:, payload:, client:)
      super

      @success_callbacks = []
      @eof_callbacks = []
      @timeout_callbacks = []
      @error_callbacks = []
    end

    def call(body = default_body)
      response = Faraday.post(config.job_url, body.to_json, client.headers)
      if response.success?
        msg_id = JSON.parse(response.body)['id']

        # registers message id for kafka responses
        response_worker.register_message(msg_id, self)

        msg_id
      else
        logger.error(receptor_log_msg("Directive #{name} failed: HTTP #{response.status}", account, node_id))
        nil
      end
    rescue Faraday::Error => e
      logger.error(receptor_log_msg("Directive #{name} failed", account, node_id, e))
      nil
    end

    def on_success(&block)
      @success_callbacks << block if block_given?
      self
    end

    def on_eof(&block)
      @eof_callbacks << block if block_given?
      self
    end

    def on_timeout(&block)
      @timeout_callbacks << block if block_given?
      self
    end

    def on_error(&block)
      @error_callbacks << block if block_given?
      self
    end

    def response_success(msg_id, message_type, response)
      if message_type == MESSAGE_TYPE_EOF
        @eof_callbacks.each { |block| block.call(msg_id) }
      else
        @success_callbacks.each { |block| block.call(msg_id, response) }
      end
    end

    def response_error(msg_id, response_code)
      @error_callbacks.each { |block| block.call(msg_id, response_code) }
    end

    def response_timeout(msg_id)
      @timeout_callbacks.each { |block| block.call(msg_id) }
    end
  end
end
