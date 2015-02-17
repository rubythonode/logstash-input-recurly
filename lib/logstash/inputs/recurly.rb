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
    require "rubygems"
    require "recurly"

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
        hash = invoice.attributes
        hash['address'] = hash['address'].attributes
        hash['created_at'] = hash['created_at'].to_s
        hash['closed_at'] = hash['closed_at'].to_s

        lineItems = hash['line_items']
        if lineItems
          lineItems.map! { |item| item.attributes }
          lineItems.each do |item|
            item['start_date'] = item['start_date'].to_s
            item['end_date'] = item['end_date'].to_s
            item['created_at'] = item['created_at'].to_s
          end
        end

        transactions = hash['transactions']
        if transactions
          transactions.map! { |item| item.attributes }
          transactions.each do |item|
            item['created_at'] = item['created_at'].to_s
            item['details']['account'] = item['details']['account'].attributes
            item['details']['account']['billing_info'] = item['details']['account']['billing_info'].attributes
          end
        end

        event = LogStash::Event.new(hash)
        decorate(event)
        queue << event
      end

      duration = Time.now - start
      @logger.info? && @logger.info("Poll completed", :command => @command, :duration => duration)
    end # loop
  end # def run
end # class LogStash::Inputs::Exec
