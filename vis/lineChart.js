var lineChartDirective = function($window, $parse) {
    // Much of the code here is from this great article: https://www.sitepoint.com/creating-charting-directives-using-angularjs-d3-js/

    return {
     restrict: "EA",
     template: '<svg width="100%" height="100%"></svg>',
     link: function(scope, elem, attrs){
          var exp = $parse(attrs.chartData);
          var dataToPlot = exp(scope)

          var padding = attrs.padding;
          var pathClass = attrs.pathClass;
          var xField = attrs.xfield
          var yField = attrs.yfield
          var xScale, yScale, xAxisGen, yAxisGen, lineFun;

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
                    redrawLineChart();
                } else {
                    drawLineChart();
                }
            } else {
                dataToPlot = []
            }
          });

          function setChartParameters() {
              xScale = d3.scale.linear()
                .domain([dataToPlot[0][xField], dataToPlot[dataToPlot.length-1][xField]])
                .range([padding, width - padding]);

              var getValue = function (d) { return d[yField]; }
              yScale = d3.scale.linear()
                .domain([Math.floor(d3.min(dataToPlot, getValue)), Math.ceil(d3.max(dataToPlot, getValue))])
                .range([height - padding, padding]);

              xAxisGen = d3.svg.axis()
                .scale(xScale)
                .orient("bottom")
                .ticks(10);

              yAxisGen = d3.svg.axis()
                .scale(yScale)
                .orient("left")
                .ticks(6)
                .tickFormat(function(d) {
                    if(d > 1) {
                        return d3.format(".2s")(d);
                    }
                    else {
                        return d3.format(".2f")(d);
                    }
                });

              lineFun = d3.svg.line()
                .x(function (d) {
                    return xScale(d[xField]);
                })
                .y(function (d) {
                    return yScale(d[yField]);
                })
                .interpolate("basis");
          }

          function drawLineChart() {
              svg.append("svg:g")
                  .attr("class", "x axis")
                  .attr("transform", "translate(0," + (height - padding) +  ")")
                  .call(xAxisGen);

              svg.append("svg:g")
                  .attr("class", "y axis")
                  .attr("transform", "translate(" + padding + ",0)")
                  .call(yAxisGen);

              svg.append("svg:path")
                  .attr({
                      d: lineFun(dataToPlot),
                      "class": pathClass
                  });
            }

            function redrawLineChart() {
                svg.selectAll("g.y.axis").call(yAxisGen);
                svg.selectAll("g.x.axis").call(xAxisGen);
                svg.selectAll("." + pathClass)
                    .attr({ d: lineFun(dataToPlot) });
            }
        }
    };
}