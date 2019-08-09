Este dataset describe la información de los campos obtenidos a partid del scraping de las propiedad a la venta en Montevideo, Uruguay desde el sitio infocasas.com.uy.

| Field | Type | Description |
|---|---|---|
| ~~acepta_permuta~~   | bool | abierto a la posibilidad de permutar por otro bien u objeto |
| ~~inmobiliaria~~           | str | Inmobiliaria |
| ~~altura_permitida~~ | int | Condiciones de la propiedad para construir edificios, según norma local. |
| ~~ambientes~~        | int | Numero de ambientes de la propiedad asociados a uso de oficina o local comercial. |
| ~~ambientes_extra~~  | bool | Si tiene 7 o más ambientes |
| ano_de_construccion  | int | Año de  construcción de la propiedad. |
| aptos_por_piso       | int | numero de apartamentos por piso. |
| banos                | int | número de años |
| banos_extra          | bool | si tiene más de 3 baños en el caso de las casa y 5 para apartamentos |
| cantidad_de_pisos    | int | cantidad de pisos del edificio |
| ~~casco~~            | str | (chacra) |
| descripcion          | str | texto descriptivo |
| ~~direccion~~        | str | dirección de la propiedad |
| disposicion          | str | disposición de la propiedad |
| distancia_al_mar     | float | metros app de la propiedad al mar |
| dormitorios          | int | numero de dormitorio |
| dormitorios_extra    | bool | más de 5 dormitorios |
| estado               | str | calidad de la propiedad (categorica ordinal) |
| extra                | str | lista de características extras como, {terraza, placard, aire} |
| ~~financia~~         | bool | acepta financiación de banco |
| garajes              | int | numero de garajes |
| garajes_extra        | bool | 3+ garage |
| gastos_comunes       | float | gastos comunes de la propiedad |
| gastos_comunes_moneda   | str | moneda de los gastos comunes |
| ~~hectareas~~        | float | número de hectáreas para las chacras |
| ~~huespedes~~        | int | numero de huespedes maximo |
| tipo_de_publicacion  | str | tipo de oferta, venta, arriendo u otros |
| ~~longitud_frente~~  | flaot | metros de frente de terreno |
| m2_de_la_terraza     | float | área de la terraza |
| m2_del_terreno       | float | área de la propiedad |
| m2_edificados        | float | area construida |
| oficina              | bool | si es para uso de oficina o no |
| penthouse            | bool | si es penthouse o no (lujo) |
| piso                 | int | puso en el que se encuentra del edificio |
| plantas              | int | numero de pisos plantas de la propiedad |
| plantas_extra        | bool | 3+ plantas |
| precio                | flaot | precio de la propiedad |
| precio_moneda            | float | moneda del precio |
| ~~referencia~~       | str | código de la propiedad |
| sobre                | str | propiedad sobre rambla, avenida, otros |
| tipo_propiedad       | str | tipo de propiedad casa, apartamento, |
| ~~titulo_publicacion~~            | str | titulo de la oferta |
| ~~url~~              | str | url de la oferta |
| vista_al_mar         | bool | tiene o no vista al mar |
| vivienda_social      | bool | si es una cooperativa o no |
| barrio                 | str | Nombre del barrio donde se encuentra la propiedad |