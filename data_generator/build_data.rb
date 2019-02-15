require 'factory_girl'
require 'faker'
require 'as-duration'
require 'ostruct'

PAIS = 'Argentina'
CIUDADES = ['Buenos Aires', 'CABA', 'Córdoba']
PROVEEDORES = ['Movistar', 'Personal', 'Claro']
CLIENT_COUNT = 10
AVG_CALLS_PER_DAY = 1
DAYS_TO_GENERATE = 10

FactoryGirl.define do
  factory :cliente, class: OpenStruct do
    sequence(:id) { |n| n }
    ciudad { CIUDADES.sample }
    pais { PAIS }
  end

  factory :telefono, class: OpenStruct do
    sequence(:id) { |n| n }
    numero { Faker::PhoneNumber.unique.cell_phone }
    cliente { clientes.sample }
    proveedor { PROVEEDORES.sample }
  end

  factory :llamada, class: OpenStruct do
    sequence(:id) { |n| n }
    hora_inicio { Faker::Time.between(DAYS_TO_GENERATE.days.ago, DateTime.now) }
    duracion { Faker::Number.normal(20, 5) }
    id_creador { telefonos.sample.id }
    id_participante do
      no_creator = telefonos.reject { |tel| tel.id == id_creador }
      no_creator.sample(2 + Faker::Number.normal(0, 0.5).abs.round).map(&:id)
    end
  end
end

clientes = FactoryGirl.build_list(:cliente, CLIENT_COUNT)
telefonos = []

clientes.each do |cliente|
  telefonos += FactoryGirl.build_list(:telefono, 1 + Faker::Number.normal(0, 0.75).round.abs, cliente: cliente)
end

telefonos_sql = telefonos.map do |telefono|
  "#{telefono.id}, #{telefono.cliente.id},'#{telefono.numero}','#{telefono.cliente.ciudad}', '#{telefono.cliente.pais}', '#{telefono.proveedor}'"
end

telefonos_file = File.open('telefonos.csv', 'w') do |file|
  file.write telefonos_sql.join("\n")
end

cantidad_llamadas = (telefonos.count * DAYS_TO_GENERATE *
  Faker::Number.normal(AVG_CALLS_PER_DAY, 0.25)).round.abs

batch_size = cantidad_llamadas / 100
llamadas = []
puts "Creando #{cantidad_llamadas} llamadas de a #{batch_size}."

(1..100).each do |i|
  puts "Batch #{i}"
  nuevas_llamadas = FactoryGirl.build_list(:llamada,
    batch_size,
    telefonos: telefonos)

  llamadas_file = File.open('llamadas.csv', 'a') do |file|
    llamadas_csv = nuevas_llamadas.map do |llamada|
      tuples = llamada.id_participante.map { |part| "#{llamada.id},#{llamada.hora_inicio},#{llamada.duracion},#{llamada.id_creador},#{part}"}
      "#{tuples.join("\n")}"
    end
    file.write llamadas_csv.join("\n")
    file.write("\n")
  end
end
