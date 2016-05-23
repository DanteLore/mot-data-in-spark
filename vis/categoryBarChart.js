var categoryBarChartDirective = function($window, $parse) {
    return {
     restrict: "EA",
     template: '<svg width="100%" height="100%"></svg>',
     link: function(scope, elem, attrs){
          var exp = $parse(attrs.chartData);
          var dataToPlot = exp(scope)

          var padding = attrs.padding;
          var colClass = attrs.colClass;
          var xField = attrs.xfield
          var yField = attrs.yfield
          var xScale, yScale, xAxisGen, yAxisGen, lineFun;
          var dataMin = 0;
          var dataMax = 0;

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
              dataMin = Math.floor(d3.min(dataToPlot, function (d) { return d[xField]; }))
              dataMax = Math.ceil(d3.max(dataToPlot, function (d) { return d[xField]; }))

              xScale = d3.scale.linear()
                .domain([dataMin, dataMax])
                .range([padding, width - (2 * padding)]);

              yScale = d3.scale.linear()
                .domain(dataToPlot.map(function(d) { return d[yField]; }))
                .range([padding, height - padding]);

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
                  .attr("transform", "translate(0," + (height - padding) +  ")")
                  .call(xAxisGen);

              svg.append("svg:g")
                  .attr("class", "y axis")
                  .attr("transform", "translate(" + padding + ",0)")
                  .call(yAxisGen);

                svg.selectAll("rect")
                    .data(dataToPlot)
                    .enter()
                    .append("rect")
                    .attr("x", function(d, i) {
                        return padding
                    })
                    .attr("y", function(d, i) {
                        //var y = i * ((height - padding - padding) / dataToPlot.length)

                        var y = yScale(d[yField])
                        return y
                    })
                    .attr("height", function(d, i) {
                        return 5
                    })
                    .attr("width", function(d, i) {
                        return xScale(d[xField])
                    })
                    .attr("class", colClass)
            }

            function redrawColumnChart() {
                svg.selectAll("g.y.axis").call(yAxisGen);
                svg.selectAll("g.x.axis").call(xAxisGen);
            }
        }
    };
}