d3.json('practica_airbnb.json')
    .then((featureCollection) => {
        drawMap(featureCollection);
    });


// Se han cargado los datos del fichero 'practica_airbnb.json'

function drawMap(featureCollection) {

    var width = 900;
    var height = 800;

    var svg = d3.select('div')
        .append('svg')
        .attr('width', width)
        .attr('height', height)
        .append('g');


    // No se ha variado apenas la definición de la creación en sí mismo del mapa (3 etapas)

    //1. Datos GeoJSON
    console.log(featureCollection)
    console.log(featureCollection.features)

    var center = d3.geoCentroid(featureCollection); //Encontrar la coordenada central del mapa (de la featureCollection)
    //var center_area = d3.geoCentroid(featureCollection.features[0]); //Encontrar la coordenada central de un area. (de un feature)

    console.log(center)

    //2.Proyeccion de coordenadas [long,lat] en valores X,Y
    var projection = d3.geoMercator()
        .fitSize([width, height], featureCollection) // equivalente a  .fitExtent([[0, 0], [width, height]], featureCollection)
        //.scale(1000)
        //Si quiero centrar el mapa en otro centro que no sea el devuelto por fitSize.
        .center(center) //centrar el mapa en una long,lat determinada
        .translate([width / 2, height / 2]) //centrar el mapa en una posicion x,y determinada

    //console.log(projection([long,lat]))

    //3.Crear paths a partir de coordenadas proyectadas.
    var pathProjection = d3.geoPath().projection(projection);
    //console.log(pathProjection(featureCollection.features[0]))
    var features = featureCollection.features;


    // Búsqueda datos precios alquiler d.properties.avgprice
    // Se calcula su intervalo con el método extent()

    var domainColor = d3.extent(features, function(d) {
        return d.properties.avgprice;
    })

    // Se puede imprimir el intervalo por con console.log(domainColor);


    // Contamos el número de barrios con un dato válido de alquiler con el método count(). Son 123 de un total de 128.


    var validos = d3.count(features, function(d) {
        return d.properties.avgprice
        
    });

    // se puede comprobar la cifra de municipios con el dato válido con console.log(validos)


    // Se define la escala de colores, que ha sido la de naranjas

    var scaleColor = d3.scaleSequential()
        .domain(domainColor)
        .range(["#fcd8b1", "#f28a18"])

   
   

    // Se implantan los paths de los barrios en el lienzo
    // De momento no tienen color

    var createdPath = svg.selectAll('path')
        .data(features)
        .enter()
        .append('path')
        .attr('d', (d) => pathProjection(d))
        .attr("opacity", function(d, i) {
            d.opacity = 1
            return d.opacity
        });

    // Comenzamos con la cororación de los 123 barrios que sí tienen dato del alquiler
    
    // Ya podemos colorear en rojo los barrios que no tienen dato del precio del alquiler, porque este color es fijo para ellos

    // Para el resto de barrios aplicamos la escala de color antes definida

    createdPath.attr('fill', function(d) {
        if (d.properties.avgprice == null) { return 'red'}
        else { return scaleColor(d.properties.avgprice)}

    })

    
    // Pasa,os a defiir la leyenda

    // Se elige un número de rectángulos en la leyenda igual a 10
    
    
    var numeroRect = 10;

    // Declaramos un array, listaPreciosRect que delimitará los colores de los rectángulos y dara los datos de la leyenda
    
    var listaPreciosRect = [];

    // Introducimos en el listaPreciosRect los valores númericos que dividen los rectángulos

    for (i=1; i<(numeroRect+1); i++) {

        listaPreciosRect.push(domainColor[0] + i*(domainColor[1]-domainColor[0])/numeroRect  )

    }


    // Se pueden comprobar el contenido de listaPreciosRect en console.log(listaPreciosRect)


    //Creacion de una leyenda que ocupe todo el width. En este caso creo una escala.

    var nblegend = 10;
    var widthRect = (width / numeroRect) - 2;
    var heightRect = 10;

    // Creamos los rectángulos de la leyenda, introduciendo como data() el array listaPreciosRect

    // Coloreamos los rectángulos aplicando también la escala de color

    var legend = svg.append("g")
        .selectAll("rect")
        .data(listaPreciosRect)
        .enter()
        .append("rect")
        .attr("width", widthRect)
        .attr("height", heightRect)
        .attr("x", (d, i) => i * widthRect ) 
        .attr("fill", (d) => scaleColor(d))
        .attr('padding', '10px')
        ;
    
    // Ahora, para poner bajo los rectángulos los valores delimitados, introducimos el valor menor del intervalo, en este caso 15
    
        listaPreciosRect.unshift(domainColor[0]);

    
    
    /*
    var scaleLegend = d3.scaleLinear()
        .domain([0, nblegend])
        .range([0, width]);

*/

    // Definimos formato para los número de la leyenda
   
    var f = d3.format(".0f")

    // Introducimos en el lienzo los números de la leyenda

    var text_legend = svg.append("g")
        .selectAll("text")
        .data(listaPreciosRect)
        .enter()
        .append("text")
        .attr("x", (d, i) => i * widthRect) 
        .attr("y", heightRect * 2.5)
        .text((d) => +f(d))
        .attr("font-size", 12)



    // Introducimos un rectángulo rojo para informar del color de los barrios que no tienen datos del alquiler

    var legendNoData = svg.append("g")
        .append("rect")
        .attr("width", widthRect)
        .attr("height", heightRect)
        .attr("x", 0) 
        .attr('y', heightRect * 5)
        .attr("fill", 'red');


    // Se añade un mensaje con el significado del color rojo (sin datos del precio del alquiler)

    var legendNoData_text = svg.append("g")
        .append("text")
        .attr("x", widthRect + 20) 
        .attr("y", heightRect * 6)
        .text('Sin datos del precio del alquiler')
        .attr("font-size", 12);


    // Creamos el elemento Tooltip que informa del nombre del barrio y su alquiler

    var tooltip = d3.select("div").append("div")
        .attr("class", "tooltip")   


    // Creamos el evento mouseover para activar el tooltip, con su funcines definidas aparte


    createdPath.on('mouseover', handleMouseOver) 

    createdPath.on('mouseout', handleMouseOut)


    // Función handleMouseOver

    
    function handleMouseOver(event, d) {

        tooltip.transition('t1')
        .duration(200)
        .style("visibility", "visible")
        // .style("opacity", .9)
        .style("left", (event.pageX + 20) + "px")
        .style("top", (event.pageY - 30) + "px")
        //.text(`Barrio: ${d.properties.name}, Alquiler: ${d.properties.avgprice} Eur`)
        .text(`Barrio: ${d.properties.name}, Alquiler: ${(d.properties.avgprice == undefined)? 'Sin datos' : (d.properties.avgprice + ' Eur')}`)
        //.text('Alquiler: ' + d.properties.avgprice)
        
    }
    

    // Función handleMouseOut

    function handleMouseOut(event, d) {

        tooltip.transition()
        .duration(200)
        .style("visibility", "hidden")
        // .style("opacity", .9)
    }



    // Creamos el evento onClick que activa la gráfica de apartamentos y dormitorios


    createdPath.on('click', handleClick) 





    // A continuación se define la función handleClick que ejecuta los gráficos


    function handleClick(event, d) {

        // El elemento clave es el código del barrio, que se puede consultar en console.log(d.properties.cartodb_id)


        // Almecenamos los datos de apartamentos y dormtorios en la variable dataBarrio

        var dataBarrio = d.properties.avgbedrooms;

        // Almecenamos los nombres de los barrios en la variable nombreBarrio

        var nombreBarrio = d.properties.name


        // Definimos los datos que delimitan cada gráfico

        var height1 = 450;
        var width1 = 350;
        var marginbottom = 120;
        var margintop = 10;

       // Creamos el svg, dentro de div denominado 'mapid'

       // Llevamos a cabo las transformaciones habituales

        var svg = d3.select('#mapid')
                .append('svg')
                .attr('width', width1+50)
                .attr('height', height1 + marginbottom + margintop)
                .append("g")
                .attr("transform", "translate(20 ," + margintop +")")


        
        
        
        //Estos comentarios que siguen incluyen los intentos de borrar los gráficos y que solo saliese uno; sin éxito
        
        //d3.select(svg).selectAll("*").remove();

        /*

        d3.selectAll('#ejex').remove()
        d3.selectAll('#ejey').remove()
        d3.selectAll('#barra').remove()

        */



        //Creacion de escalas de forma habitual

        // Comenzamos con la escala del eje x

         var xscale = d3.scaleBand()
        .domain(dataBarrio.map(function(d) {
        return d.bedrooms;
        }))
        .range([0, width1])
        .padding(0.1);



        // Calculamos valores máximo, mínimo, sumas apartamentos y habitaciones

        // Utilizaremos estos datos para coloreal los rectángulos y dar más datos en la parte inferior del gráfico

        var maximo = d3.max(dataBarrio, function(d) {
            return d.total
        });

        var minimo = d3.min(dataBarrio, function(d) {
            return d.total
        });

        var sumaApat = d3.sum(dataBarrio, function(d) {
            return d.total
        });

        var sumaHab = d3.sum(dataBarrio, function(d) {
            return d.total * d.bedrooms
        });


        
        // Utlizamos el valor de máximo antes calculado para definor la escala del eje y

        var yscale = d3.scaleLinear()
        .domain([0, maximo])
        .range([height1, 150]);


        // Creación de eje X en la forma habitual

        var xaxis = d3.axisBottom(xscale);

        

        // Creación de eje Y en la forma habitual

        var yaxis = d3.axisLeft(yscale);


        // Añadimos el eje X

        svg.append("g")
        .attr('id', 'ejex')
        .attr("transform", "translate(20," + height1 + ")")
        .call(xaxis);


        // Añadimos el eje Y

        svg.append("g")
        .attr('id', 'ejey')
        .attr("transform", "translate(20, 0)")
        .call(yaxis);


        // Creacion de los rectangulos de forma habitual

        // Los definimos con la opacidad cero, para luego hacer una transición

        var rect = svg
        .selectAll('rect')
        .data(dataBarrio)
        .enter()
        .append('rect')
        .attr('id', 'barra')
        .attr('opacity', 0)
        
       

        // Definimos el color de los rectángulos: rojo el del número de dormitorios más habitual, azul el que menos, y verde el resto

        rect.attr('fill', (e) => {
            if (e.total == maximo) {
                
                return 'red'
            } else if (e.total == minimo){
                return 'blue'
            } else {
                
                return '#93CAAE'
            }
        });


        // Introduciomos los rectángulos en la gráfica

        rect
            .attr("x", function(d) {
                return xscale(d.bedrooms) + 20
                
            })
            .attr('y', d => {
            return yscale(d.total)
            })
            .attr("width", xscale.bandwidth())
            .attr("height", function(d) {
            return height1 - yscale(d.total); 

            });



            // Hacemos la transición de opacidad cero a uno de los rectángulos

            rect.transition('t2')
            .duration(1500)
            .attr('opacity', 1);


            // Ahora añadimos información en la parte inferior del gráfico


            var f = d3.format(".1f")


            // Leyenda del eje x

            var text1 = svg.append('text')
            .text('Dormitorios en cada apartamento')
            .attr('x', width1 / 2 - 80)
            .attr('y', height1 + 35);


            // Información del nombre de barrio, número de apartamentos y media de dormitorios por apartamento

            var text2 = svg.append('text')
            .text('Barrio: ' + nombreBarrio)
            .attr('x', 0)
            .attr('y', height1 + 60);

            var text3 = svg.append('text')
            .text('Apartamentos: ' + sumaApat)
            .attr('x', 0)
            .attr('y', height1 + 85);

            var text4 = svg.append('text')
            .text('Media de Dormitorios por apartamento: ' + +f(sumaHab / sumaApat))
            .attr('x', 0)
            .attr('y', height1 + 110);


            // Introducimos un tooltip en los rectángulos para informar de los dormitorios y apartamentos


            var tooltip1 = d3.select("div").append("div")
            .attr("class", "tooltip") 


            // Activamos el tooltip con mouseover y mouseout, que definimos en funciones aparte que se presentan a continuación


            rect.on('mouseover', handleMouseOver1) 
            
            rect.on('mouseout', handleMouseOut1)

    
            function handleMouseOver1(event, d) {

                tooltip1.transition('t3')
                .duration(200)
                .style("visibility", "visible")
                
                .style("left", (event.pageX + 0) + "px")
                .style("top", (event.pageY - 0) + "px")
                .text(d.total + ' apartamento(s) de ' + d.bedrooms + ' habitacion(es)' )
            

            }

            function handleMouseOut1(event, d) {

                tooltip1.transition()
                .duration(200)
                .style("visibility", "hidden")
                
            }


        // Y eso es todo

    }

}