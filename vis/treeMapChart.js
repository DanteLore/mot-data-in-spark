
// Much from here: http://bl.ocks.org/davetaz/9954190
// And here: https://bl.ocks.org/mbostock/4063582

var treeMapChartDirective = function($window, $parse) {
    return {
     restrict: "EA",
     template: '<svg width="100%" height="100%"></svg>',
     link: function(scope, elem, attrs){
          var exp = $parse(attrs.chartData);
          var allData = exp(scope)

          var padding = parseInt(attrs.padding);
          var nodeClass = attrs.nodeClass
          var sizeField = attrs.sizeField
          var nameField = attrs.nameField
          var colourField = attrs.colourField
          var xScale, yScale, xAxisGen, yAxisGen, lineFun;
          var dataMin = 0;
          var dataMax = 0;

          var d3 = $window.d3;
          var rawSvg = elem.find('svg');
          var svg = d3.select(rawSvg[0]);

          var width = parseInt(svg.style("width"));
          var height = parseInt(svg.style("height"));

          var categoryScale = d3.scale.category20c();

          var xScale = d3.scale.linear()
              .domain([0, 1])
              .range([0, width - padding - padding]);

          var yScale = d3.scale.linear()
              .domain([0, 1])
              .range([0, height - padding - padding - padding]);

          var current = null
          var treeMap = d3.layout.treemap()
                .children(function(d, depth) {
                        return depth ? null : d.children;
                    }
                )
                //.ratio(0.6)
	            .ratio(height / width)
                .size([ 1, 1 ])
                .round(false)
                .sticky(true)
                .value(function(x) { return x[sizeField] })

          scope.$watchCollection(exp, function(newVal, oldVal){
            if(newVal) {
                allData = newVal;

                var nodes = allData;
                layout(nodes)
                drawChart(nodes);
            }
            else {
                allData = []
            }
          });

            function layout(d) {
                if (d.children) {
                    treeMap.nodes({children: d.children});
                    d.children.forEach(function(c) {
                        c.x = d.x + c.x * d.dx;
                        c.y = d.y + c.y * d.dy;
                        c.dx *= d.dx;
                        c.dy *= d.dy;
                        c.parent = d;
                        layout(c);
                    });
                }
            }

          function drawChart(treeData) {
            current = treeData.parent

            svg.selectAll("rect").remove();
            svg.selectAll("text").remove();

            svg
                .append("rect")
                .attr("class", nodeClass)
                .attr("left", 0)
                .attr("top", 0)
                .attr("height", padding)
                .attr("width", width)
                .attr("class", nodeClass)
                .text("hello!")
                .on("click", function() {
                    if(current) {
                        if(current.x && current.y){
                            xScale.domain([current.x, current.x + current.dx]);
                            yScale.domain([current.y, current.y + current.dy]);
                        }
                        else {
                            xScale.domain([0, 1]);
                            yScale.domain([0, 1]);
                        }

                        drawChart(current);
                    }
                });

            svg
               .selectAll("rect")
               .data(treeData.children)
               .enter()
               .append("rect")
               .attr("x", function(d) {
                   return xScale(d.x) + padding;
               })
               .attr("y", function(d) {
                   return yScale(d.y) + padding + padding;
               })
               .attr("width", function(d) {
                   return xScale(d.x + d.dx) - xScale(d.x)
               })
               .attr("height", function(d) {
                   return yScale(d.y + d.dy) - yScale(d.y);
               })
               .attr("class", nodeClass)
               .style("fill", function(d) {
                    if(colourField) {
                        return d[colourField];
                    }
                    else {
                        var c = categoryScale(d[nameField]);
                        return c;
                    }
               })
               .on("click", function(d) {
                if(d.children && d.children.length > 0) {
                        xScale.domain([d.x, d.x + d.dx]);
                        yScale.domain([d.y, d.y + d.dy]);
                        drawChart(d);
                    }
               });

            svg
               .selectAll("text")
               .data(treeData.children)
               .enter()
               .append("text")
               .text(function(d) { return d[nameField]; })
               .attr("class", "text")
               .attr("x", function(d) {
                   return xScale(d.x) + padding + 4
               })
               .attr("y", function(d) {
                   return yScale(d.y) + padding + padding + 12
               });
          }
        }
    };
}