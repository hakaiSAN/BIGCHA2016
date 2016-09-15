Rails.application.routes.draw do
  root 'market#index'
  get 'market/index'

  # For details on the DSL available within this file, see http://guides.rubyonrails.org/routing.html
end
