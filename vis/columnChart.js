var columnChartDirective = function($window, $parse) {
    return {
     restrict: "EA",
     template: '<svg width="100%" height="100%"></svg>',
     link: function(scope, elem, attrs){
          var exp = $parse(attrs.chartData);
          var dataToPlot = exp(scope)

          var padding = parseInt(attrs.padding);
          var colClass = attrs.colClass;
          var xField = attrs.xfield
          var yField = attrs.yfield
          var xScale, yScale, xAxisGen, yAxisGen, lineFun;
          var dataMin = 0;
          var dataMax = 0;
          var barWidth = 0;

          var d3 = $window.d3;
          var rawSvg = elem.find('svg');
          var svg = d3.select(rawSvg[0]);

          var width = parseInt(svg.style("width"));
          var height = parseInt(svg.style("height"));

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
              barWidth = ((width - padding - padding) / dataToPlot.length) - 2

              xMin = Math.floor(d3.min(dataToPlot, function (d) { return d[xField]; }))
              xMax = Math.ceil(d3.max(dataToPlot, function (d) { return d[xField]; }))

              xScale = d3.scale.linear()
                .domain([xMin, xMax])
                .range([padding + (barWidth / 2) + 2, width - padding]);

              dataMin = Math.floor(d3.min(dataToPlot, function (d) { return d[yField]; }))
              dataMax = Math.ceil(d3.max(dataToPlot, function (d) { return d[yField]; }))

              yScale = d3.scale.linear()
                .domain([dataMin, dataMax])
                .range([height - padding - 1, padding]);

              xAxisGen = d3.svg.axis()
                .scale(xScale)
                .orient("bottom")
                .ticks(10);

              yAxisGen = d3.svg.axis()
                .scale(yScale)
                .orient("left")
                .ticks(10)
                .tickFormat(function(d) {return d3.format(".2s")(d); });
          }

          function drawColumnChart() {
              svg.append("svg:g")
                  .attr("class", "x axis")
                  .attr("transform", "translate(0," + (height - padding) +  ")")
                  .call(xAxisGen);

              svg.append("svg:g")
                  .attr("class", "y axis")
                  .attr("transform", "translate(" + padding + ",0)")
                  .call(yAxisGen);

              //svg.append("svg:path")
              //    .attr({
              //        d: lineFun(dataToPlot),
              //        "class": pathClass
              //    });

                svg.selectAll("rect")
                    .data(dataToPlot)
                    .enter()
                    .append("rect")
                    .attr("x", function(d, i) {
                        return xScale(d[xField]) - (barWidth / 2)
                    })
                    .attr("y", function(d, i) {
                        return yScale(d[yField])
                    })
                    .attr("height", function(d, i) {
                        return yScale(dataMin) - yScale(d[yField])
                    })
                    .attr("width", function(d, i) {
                        return barWidth
                    })
                    .attr("class", colClass)
            }

            function redrawColumnChart() {
                svg.selectAll("g.y.axis").call(yAxisGen);
                svg.selectAll("g.x.axis").call(xAxisGen);
                // svg.selectAll("." + pathClass)
                //    .attr({ d: lineFun(dataToPlot) });
            }
        }
    };
}