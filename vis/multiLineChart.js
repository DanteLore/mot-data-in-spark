var multiLineChartDirective = function($window, $parse) {
    return {
     restrict: "EA",
     template: '<svg width="100%" height="100%"></svg>',
     link: function(scope, elem, attrs){
          var d3 = $window.d3;
          var exp = $parse(attrs.chartData);
          var dataToPlot = exp(scope)

          var padding = parseInt(attrs.padding);
          var pathClass = attrs.pathClass;
          var xField = attrs.xfield
          var yField = attrs.yfield
          var nameField = attrs.namefield
          var xScale, yScale, xAxisGen, yAxisGen, lineFun;

          var categoryScale = d3.scale.category20c();
          var rawSvg = elem.find('svg');
          var svg = d3.select(rawSvg[0]);

          var width = parseInt(svg.style("width"));
          var height = parseInt(svg.style("height"));

          var tip = d3.tip()
              .attr('class', 'd3-tip')
              .offset([0, 0])
              .html(function(d) {
                return "<strong>" + d[nameField] + "</strong>";
              })
          svg.call(tip);

          scope.$watchCollection(exp, function(newVal, oldVal){
            if(newVal) {
                dataToPlot = newVal;
                setChartParameters();

                if(oldVal) {
                    redrawLineChart();
                } else {
                    drawLineChart();
                }
            }
            else {
                dataToPlot = []
            }
          });

          function setChartParameters() {
              var getYValue = function (d) { return d[yField]; }
              yMax = d3.max(dataToPlot.map(function(row) { return d3.max(row.series, getYValue) }))
              yMin = d3.min(dataToPlot.map(function(row) { return d3.min(row.series, getYValue) }))

              xScale = d3.scale.linear()
                .domain([3, 20])
                .range([padding * 2, width - padding]);

              yScale = d3.scale.linear()
                .domain([yMin, yMax])
                .range([height - padding, padding])
                .nice();

              xAxisGen = d3.svg.axis()
                .scale(xScale)
                .orient("bottom");

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
                  .attr("transform", "translate(" + (padding * 2) + ",0)")
                  .call(yAxisGen);

              svg.append("svg:g")
                    .selectAll("path")
                    .data(dataToPlot)
                    .enter()
                    .append("path")
                    .attr("d", function(d) { return lineFun(d.series); })
                    .attr("class", pathClass)
                    .style("stroke", function(d) {
                        return categoryScale(d.make)
                    })
                    .style("opacity", "0.25")
                    .on("mouseover", function (d) {
                        d3.select(this)
                        .style("stroke-width",'6px')
                        .style("opacity", "1.0")

                        tip.show(d)
                    })
                    .on("mouseout", function (d) {
                        d3.select(this)
                        .style("stroke-width","")
                        .style("opacity", "0.25")

                        tip.hide(d)
                    })
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