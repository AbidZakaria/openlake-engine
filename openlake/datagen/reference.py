"""Reference data used to make the synthetic marketplace look like the real one."""

from __future__ import annotations

# State -> (population weight, representative cities, zip prefix range, distance tier from São Paulo)
STATES: dict[str, tuple[float, list[str], tuple[int, int], int]] = {
    "SP": (0.420, ["sao paulo", "campinas", "guarulhos", "santos", "sao bernardo do campo", "ribeirao preto", "sorocaba", "osasco"], (1000, 19999), 0),
    "RJ": (0.129, ["rio de janeiro", "niteroi", "duque de caxias", "nova iguacu", "petropolis"], (20000, 28999), 1),
    "MG": (0.117, ["belo horizonte", "uberlandia", "contagem", "juiz de fora", "betim"], (30000, 39999), 1),
    "RS": (0.055, ["porto alegre", "caxias do sul", "pelotas", "canoas"], (90000, 99999), 2),
    "PR": (0.051, ["curitiba", "londrina", "maringa", "ponta grossa"], (80000, 87999), 1),
    "SC": (0.037, ["florianopolis", "joinville", "blumenau", "itajai"], (88000, 89999), 2),
    "BA": (0.034, ["salvador", "feira de santana", "vitoria da conquista"], (40000, 48999), 3),
    "DF": (0.021, ["brasilia"], (70000, 73699), 2),
    "ES": (0.020, ["vitoria", "vila velha", "serra"], (29000, 29999), 2),
    "GO": (0.020, ["goiania", "anapolis", "aparecida de goiania"], (72800, 76799), 2),
    "PE": (0.017, ["recife", "jaboatao dos guararapes", "olinda"], (50000, 56999), 3),
    "CE": (0.013, ["fortaleza", "caucaia", "juazeiro do norte"], (60000, 63999), 3),
    "PA": (0.010, ["belem", "ananindeua", "santarem"], (66000, 68899), 4),
    "MT": (0.009, ["cuiaba", "varzea grande", "rondonopolis"], (78000, 78899), 3),
    "MA": (0.007, ["sao luis", "imperatriz"], (65000, 65999), 4),
    "MS": (0.007, ["campo grande", "dourados"], (79000, 79999), 2),
    "PB": (0.005, ["joao pessoa", "campina grande"], (58000, 58999), 3),
    "PI": (0.005, ["teresina", "parnaiba"], (64000, 64999), 4),
    "RN": (0.005, ["natal", "mossoro"], (59000, 59999), 3),
    "AL": (0.004, ["maceio", "arapiraca"], (57000, 57999), 3),
    "SE": (0.003, ["aracaju"], (49000, 49999), 3),
    "TO": (0.003, ["palmas", "araguaina"], (77000, 77999), 3),
    "RO": (0.0025, ["porto velho", "ji-parana"], (76800, 76999), 4),
    "AM": (0.0015, ["manaus"], (69000, 69299), 4),
    "AC": (0.0008, ["rio branco"], (69900, 69999), 4),
    "AP": (0.0007, ["macapa"], (68900, 68999), 4),
    "RR": (0.0005, ["boa vista"], (69300, 69399), 4),
}

# Portuguese category -> (English translation, demand weight, median price BRL, median weight g)
CATEGORIES: dict[str, tuple[str, float, float, int]] = {
    "cama_mesa_banho": ("bed_bath_table", 0.099, 79, 1500),
    "beleza_saude": ("health_beauty", 0.087, 110, 800),
    "esporte_lazer": ("sports_leisure", 0.078, 95, 1200),
    "moveis_decoracao": ("furniture_decor", 0.074, 85, 2500),
    "informatica_acessorios": ("computers_accessories", 0.069, 120, 900),
    "utilidades_domesticas": ("housewares", 0.062, 70, 1600),
    "relogios_presentes": ("watches_gifts", 0.053, 199, 500),
    "telefonia": ("telephony", 0.040, 60, 300),
    "ferramentas_jardim": ("garden_tools", 0.039, 90, 2400),
    "automotivo": ("auto", 0.037, 110, 1800),
    "brinquedos": ("toys", 0.036, 80, 1300),
    "cool_stuff": ("cool_stuff", 0.033, 150, 1900),
    "perfumaria": ("perfumery", 0.030, 120, 450),
    "bebes": ("baby", 0.027, 115, 2100),
    "eletronicos": ("electronics", 0.025, 75, 500),
    "papelaria": ("stationery", 0.022, 70, 900),
    "fashion_bolsas_e_acessorios": ("fashion_bags_accessories", 0.018, 65, 400),
    "pet_shop": ("pet_shop", 0.017, 95, 2000),
    "moveis_escritorio": ("office_furniture", 0.015, 160, 9000),
    "consoles_games": ("consoles_games", 0.010, 140, 700),
    "malas_acessorios": ("luggage_accessories", 0.010, 160, 3500),
    "construcao_ferramentas_construcao": ("construction_tools_construction", 0.009, 120, 3000),
    "eletrodomesticos": ("home_appliances", 0.008, 110, 2500),
    "instrumentos_musicais": ("musical_instruments", 0.006, 210, 3200),
    "eletroportateis": ("small_appliances", 0.006, 260, 4000),
    "casa_construcao": ("home_construction", 0.005, 120, 3200),
    "livros_interesse_geral": ("books_general_interest", 0.005, 60, 600),
    "alimentos": ("food", 0.004, 55, 900),
    "moveis_sala": ("furniture_living_room", 0.004, 180, 8000),
    "audio": ("audio", 0.003, 130, 700),
    "climatizacao": ("air_conditioning", 0.003, 190, 5000),
    "pcs": ("computers", 0.002, 1100, 6000),
}

REVIEW_TITLES = ["recomendo", "otimo produto", "muito bom", "chegou antes do prazo", "nao recebi",
                 "produto com defeito", "bom", "excelente", "demorou muito", "super recomendo"]
REVIEW_MESSAGES = [
    "produto chegou dentro do prazo e bem embalado",
    "gostei muito, qualidade excelente",
    "entrega rapida, recomendo a loja",
    "ainda nao recebi o produto",
    "o produto veio diferente do anunciado",
    "chegou com defeito, aguardando troca",
    "bom custo beneficio",
    "atendimento otimo, voltarei a comprar",
    "demorou mais que o previsto mas chegou",
    "produto de qualidade, vendedor atencioso",
]

ORDER_STATUSES = ["delivered", "shipped", "canceled", "unavailable", "invoiced", "processing", "created", "approved"]
ORDER_STATUS_WEIGHTS = [0.970, 0.011, 0.006, 0.006, 0.003, 0.003, 0.0005, 0.0005]
