Rails.application.routes.draw do
  root 'market#index'
  get 'market/index'
  post 'market/index'
  get 'market/list'

  # For details on the DSL available within this file, see http://guides.rubyonrails.org/routing.html
end
