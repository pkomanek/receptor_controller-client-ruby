require "faraday"

module ReceptorController
  class Client::Directive
    attr_accessor :name, :account, :node_id, :payload, :client

    delegate :config, :logger, :receptor_log_msg, :response_worker, :to => :client

    EOF_MESSAGE_TYPE = 'eof'.freeze

    def initialize(name:, account:, node_id:, payload:, client:)
      self.account         = account
      self.client          = client
      self.name            = name
      self.node_id         = node_id
      self.payload         = payload
    end

    def call
      raise NotImplementedError, "#{__method__} must be implemented in a subclass"
    end

    def on_success(&block)
      @on_response ||= []
      @on_response << block if block_given?
      @on_response
    end

    def on_eof(&block)
      @on_eof ||= []
      @on_eof << block if block_given?
      @on_eof
    end

    def on_timeout(&block)
      @on_timeout ||= []
      @on_timeout << block if block_given?
      @on_timeout
    end

    def on_error(&block)
      @on_error ||= []
      @on_error << block if block_given?
      @on_error
    end

    def response_success(msg_id, message_type, response)
      if message_type == EOF_MESSAGE_TYPE
        on_eof.each { |block| block.call(msg_id) }
      else
        on_success.each { |block| block.call(msg_id, response) }
      end
    end

    def response_error(msg_id, response_code)
      on_error.each { |block| block.call(msg_id, response_code) }
    end

    def response_timeout(msg_id)
      on_timeout.each { |block| block.call(msg_id) }
    end
  end
end
