require "receptor_controller/client"

RSpec.describe ReceptorController::Client do
  let(:external_tenant) {'0000001'}
  let(:organization_id) {'000001'}
  let(:identity) do
    {"x-rh-identity" => Base64.strict_encode64({"identity" => {"account_number" => external_tenant, "user" => {"is_org_admin" => true}, "internal" => {"org_id" => organization_id}}}.to_json)}
  end
  let(:headers) do
    {"Content-Type"    => "application/json",
     "User-Agent"      => "Faraday v1.0.1",
     "Accept"          => "*/*",
     "Accept-Encoding" => 'gzip;q=1.0,deflate;q=0.6,identity;q=0.3'}.merge(identity)
  end
  let(:receptor_scheme) { 'http' }
  let(:receptor_host) { 'localhost:9090' }
  let(:receptor_node) { 'testing-receptor' }
  let(:receptor_config) do
    ReceptorController::Client::Configuration.new do |config|
      config.controller_scheme = receptor_scheme
      config.controller_host   = receptor_host
    end
  end
  let(:satellite_uid) {'1234567890'}

  subject {described_class.new(:config => receptor_config)}

  before do
    subject.identity_header = identity
  end

  describe "#connection_status" do
    it "makes a POST request to receptor and returns status key-value if successful" do
      response = {"status" => "connected"}
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/connection/status")
        .with(:body    => {"account" => external_tenant, "node_id" => receptor_node}.to_json,
              :headers => headers)
        .to_return(:status => 200, :body => response.to_json, :headers => {})

      expect(subject.connection_status(external_tenant, receptor_node)).to eq(response)
    end

    it "makes a POST request to receptor and returns disconnected status in case of error" do
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/connection/status")
        .with(:body    => {"account" => external_tenant, "node_id" => receptor_node}.to_json,
              :headers => headers)
        .to_return(:status => 401, :body => {"errors" => [{"status" => 401, "detail" => "Unauthorized"}]}.to_json, :headers => {})

      expect(subject.connection_status(external_tenant, receptor_node)).to eq(described_class::STATUS_DISCONNECTED)
    end

    it "makes a POST request and returns disconnected if receptor unavailable" do
      allow(Faraday).to receive(:post).and_raise(Faraday::ConnectionFailed, "Failed to open TCP connection to #{receptor_host}")

      expect(subject.connection_status(external_tenant, receptor_node)).to eq(described_class::STATUS_DISCONNECTED)
    end
  end

  describe "#send_directive" do
    let(:caller) {double("Caller object")}
    let(:payload) {{'satellite_instance_id' => satellite_uid.to_s}.to_json}
    let(:directive) {'receptor_satellite:health_check'}

    it "makes POST /job request to receptor, registers received message ID and returns it" do
      response  = {"id" => '1234'}

      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
        .with(:body    => {:account   => external_tenant,
                           :recipient => receptor_node,
                           :payload   => payload,
                           :directive => directive}.to_json,
              :headers => headers)
        .to_return(:status => 200, :body => response.to_json, :headers => {})

      expect(subject.send(:response_worker))
        .to receive(:register_message)
              .with(response['id'], caller, :response_callback => :response_received, :timeout_callback => :response_timeout)

      expect(subject.send_directive(external_tenant, receptor_node,
                                    :payload         => payload,
                                    :directive       => directive,
                                    :response_object => caller)).to eq(response['id'])
    end

    it "makes POST /job request to receptor, doesn't register message and returns nil in case of error" do
      stub_request(:post, "#{receptor_scheme}://#{receptor_host}/job")
        .with(:body    => {:account   => external_tenant,
                           :recipient => receptor_node,
                           :payload   => payload,
                           :directive => directive}.to_json,
              :headers => headers)
        .to_return(:status => 401, :body => {"errors" => [{"status" => 401, "detail" => "Unauthorized"}]}.to_json, :headers => {})

      expect(subject.send(:response_worker)).not_to receive(:register_message)

      expect(subject.send_directive(external_tenant, receptor_node,
                                    :payload         => payload,
                                    :directive       => directive,
                                    :response_object => caller)).to be_nil

    end

    it "makes a POST request and returns disconnected if receptor unavailable" do
      allow(Faraday).to receive(:post).and_raise(Faraday::ConnectionFailed, "Failed to open TCP connection to #{receptor_host}")

      expect(subject.send(:response_worker)).not_to receive(:register_message)

      expect(subject.send_directive(external_tenant, receptor_node,
                                    :payload         => payload,
                                    :directive       => directive,
                                    :response_object => caller)).to be_nil
    end
  end
end
