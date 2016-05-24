var categoryBarChartDirective = function($window, $parse) {
    return {
     restrict: "EA",
     template: '<svg width="100%" height="100%"></svg>',
     link: function(scope, elem, attrs){
          var exp = $parse(attrs.chartData);
          var dataToPlot = exp(scope)

          var padding =  parseInt(attrs.padding);
          var colClass = attrs.colClass;
          var xField = attrs.xfield
          var yField = attrs.yfield
          var colourField = attrs.colourField
          var xScale, yScale, xAxisGen, yAxisGen, lineFun;
          var dataMin = 0;
          var dataMax = 0;

          var d3 = $window.d3;
          var rawSvg = elem.find('svg');
          var svg = d3.select(rawSvg[0]);

          var width = parseInt(svg.style("width"));
          var height = parseInt(svg.style("height"));
          var yHeight = 0
          var yCount = 0
          var barHeight = 0

          scope.$watchCollection(exp, function(newVal, oldVal){
            if(newVal) {
                dataToPlot = newVal;
                setChartParameters();

                if(oldVal) {
                    redrawColumnChart();
                } else {
                    drawColumnChart();
                }
            } else {
                dataToPlot = []
            }
          });

          function setChartParameters() {
              dataMin = Math.floor(d3.min(dataToPlot, function (d) { return d[xField]; }))
              dataMax = Math.ceil(d3.max(dataToPlot, function (d) { return d[xField]; }))

              yHeight = height - padding - padding
              yCount = dataToPlot.length
              barHeight = (yHeight / yCount) - 4

              xScale = d3.scale.linear()
                .domain([dataMin, dataMax])
                .range([0, width - (3 * padding)]);

              yScale = d3.scale.ordinal()
                .domain(dataToPlot.map(function(d) { return d[yField]; }))
                .range(dataToPlot.map(function(d, i) { return ((i * yHeight) / yCount) + padding; }) );

              xAxisGen = d3.svg.axis()
                .scale(xScale)
                .orient("bottom")
                .ticks(6)
                .tickFormat(function(d) {return d3.format(".2s")(d); });

              yAxisGen = d3.svg.axis()
                .scale(yScale)
                .orient("left");
          }

          function drawColumnChart() {
              svg.append("svg:g")
                  .attr("class", "x axis")
                  .attr("transform", "translate(" + padding * 2 + "," + (height - padding) +  ")")
                  .call(xAxisGen);

              svg.append("svg:g")
                  .attr("class", "y axis")
                  .attr("transform", "translate(" + padding * 2 + "," + barHeight / 2 + ")")
                  .call(yAxisGen);

                svg.selectAll("rect")
                    .data(dataToPlot)
                    .enter()
                    .append("rect")
                    .attr("x", function(d, i) {
                        return (padding * 2) + 1
                    })
                    .attr("y", function(d, i) {
                        return yScale(d[yField])
                    })
                    .attr("height", function(d, i) {
                        return barHeight
                    })
                    .attr("width", function(d, i) {
                        return xScale(d[xField])
                    })
                    .attr("class", colClass)
                    .attr("style", function(d, i) {
                        var style = "fill: " + d[colourField] + ";"
                        if(d[colourField] == "white") {
                            style = style + " stroke: black; stroke-width: 1px;"
                        }

                        return style
                    })
            }

            function redrawColumnChart() {
                svg.selectAll("g.y.axis").call(yAxisGen);
                svg.selectAll("g.x.axis").call(xAxisGen);
            }
        }
    };
}