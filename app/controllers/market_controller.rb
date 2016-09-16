class MarketController < ApplicationController
  require "open3"
  require "csv"
  def index
    if request.post?
      @brand_names = CSV.read("app/assets/csv/brand_name.csv")
      #handle posts
      @part = params[:part]
      @brand = params[:brand]
      # out, err, status = Open3.capture3('yarn jar Avedev-0.0-1.1-SNAPSHOT-jar-with-dependencies.jar jp.ac.nii.Avedev ave/input/merge.csv hadoop/output "'+ @brand +','+ @part +'"')
      # @hadoop = status
      # if @hadoop.exitstatus!=0 then return 0
      # end
      @ave_arrs = CSV.read("app/assets/csv/"+@brand+"_"+@part+"_average.csv")
      @dev_arrs = CSV.read("app/assets/csv/"+@brand+"_"+@part+"_deviation.csv")
    else
      #handle gets
      @ave_arrs = [0,0]
      @dev_arrs = [0,0]
      @brand_names = CSV.read("app/assets/csv/brand_name.csv")
      @part = 'p'
    end
  end
  # def list
  #   data =
  #   render :json => data
  # end
end
