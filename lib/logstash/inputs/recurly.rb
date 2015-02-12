# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

# Pull invoices from Recurly API.
#
# List invoices: GET https://:subdomain.recurly.com/v2/invoices
class LogStash::Inputs::Recurly < LogStash::Inputs::Base

  config_name "recurly"

  milestone 1

  # Interval to run the command. Value is in seconds.
  config :interval, :validate => :number, :required => true

  # API key
  config :api_key, :validate => :string, :required => true

  # Subdomain
  #
  # The subdomain for which data you want to access.
  config :subdomain, :validate => :string, :required => true

  public
  def register
    require "json"
    require 'recurly'

    Recurly.subdomain      = @subdomain
    Recurly.api_key        = @api_key

    # To set a default currency for your API requests:
    Recurly.default_currency = 'USD'

    @logger.info? && @logger.info("Registering Recurly Input", :subdomain => @subdomain, :interval => @interval)
  end # def register

  public
  def run(queue)
    Stud.interval(@interval) do
      start = Time.now
      @logger.info? && @logger.info("Polling Recurly", :time => start)

      Recurly::Invoice.find_each do |invoice|
        puts "Invoice: #{invoice.inspect}"

        event = LogStash::Event.new(invoice)
        decorate(event)
        queue << event
      end

      duration = Time.now - start
      @logger.info? && @logger.info("Poll completed", :command => @command, :duration => duration)
    end # loop
  end # def run
end # class LogStash::Inputs::Exec
