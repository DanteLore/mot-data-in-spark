var heatMatrixChart = function($window, $parse) {
    return {
     restrict: "EA",
     template: '<svg width="100%" height="100%"></svg>',
     link: function(scope, elem, attrs){
          var exp = $parse(attrs.chartData);
          var matrix = exp(scope)

          var padding = attrs.padding;
          var xScale, yScale, colScale;
          var maxValue = 0;
          var minValue = 0;

          var d3 = $window.d3;
          var rawSvg = elem.find('svg');
          var svg = d3.select(rawSvg[0]);

          var width = parseInt(svg.style("width"));
          var height = parseInt(svg.style("height"));

          scope.$watchCollection(exp, function(newVal, oldVal){
            if(newVal) {
                matrix = newVal;
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

          Array.prototype.max = function() {
            return Math.max.apply(null, this);
          };

          Array.prototype.min = function() {
            return Math.min.apply(null, this);
          };

          function setChartParameters() {
              maxValue = matrix.map(function(row) { return row.max() }).max()
              minValue = matrix.map(function(row) { return row.min() }).min()

              xScale = d3.scale.linear()
                .domain([0, matrix[0].length])
                .range([padding, width - padding]);

              yScale = d3.scale.linear()
                .domain([0, matrix.length])
                .range([padding, height - padding]);

              colScale = d3.scale.linear()
                  .domain([minValue, maxValue])
                  .range(["#e5f5f9", "#2ca25f"]);
          }

          function drawColumnChart() {
              svg.append("svg:g")
                    .selectAll("g")
                    .data(matrix)
                    .enter()
                    .append("g")
                    .selectAll("rect")
                    .data( function(d, i, j) { return d; } )
                    .enter()
                    .append("rect")
                    .attr("x", function(d, i, j) {
                        return xScale(i)
                    })
                    .attr("y", function(d, i, j) {
                        return yScale(j)
                    })
                    .attr("height", function(d) {
                        return yScale(1) - yScale(0)
                    })
                    .attr("width", function(d) {
                        return xScale(1) - xScale(0)
                    })
                    .style("fill", function(d, i, j) {
                        return colScale(d)
                    })
                    .style("stroke", function(d, i, j) {
                            return colScale(d)
                    })
            }
        }
    };
}