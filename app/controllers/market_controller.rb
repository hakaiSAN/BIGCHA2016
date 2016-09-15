class MarketController < ApplicationController
  require "open3"
  require 'csv'
  def index
    out, err, status = Open3.capture3("echo kokodehadoop")
    @hadoop = status
    if @hadoop.exitstatus!=0 then return 0
    end
    # csv_data = CSV.read('av.csv', headers: true)
    @brand_names = CSV.read("app/assets/csv/brand_name.csv")
    @ave_arrs = CSV.read("app/assets/csv/av.csv")
    @dev_arrs = CSV.read("app/assets/csv/hen.csv")

  end
end
