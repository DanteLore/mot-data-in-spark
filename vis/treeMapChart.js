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
              .domain([0, width])
              .range([0, width]);

          var yScale = d3.scale.linear()
              .domain([0, height])
              .range([0, height]);


          var treeMap = d3.layout.treemap()
                .children(function(d, depth) {
                    return depth ? null : d.children; }
                )
                .size([ width - padding - padding, height - padding - padding ])
                .sticky(true)
                .value(function(x) { return x[sizeField] })

          scope.$watchCollection(exp, function(newVal, oldVal){
            if(newVal) {
                allData = newVal;

                drawChart(allData);
            }
            else {
                allData = []
            }
          });

          function drawChart(treeData) {

            svg.datum(treeData)
               .selectAll("rect")
               .data(treeMap.nodes)
               .enter()
               .append("rect")
               .attr("x", function(d) {
                   return xScale(d.x) + padding
               })
               .attr("y", function(d) {
                   return yScale(d.y) + padding
               })
               .attr("width", function(d) {
                   return xScale(d.dx)
               })
               .attr("height", function(d) {
                   return yScale(d.dy)
               })
               .attr("class", nodeClass)
               .style("fill", function(d) {
                    if(colourField) {
                        return d[colourField];
                    }
                    else {
                        return categoryScale(d[nameField])
                    }
               })
               /*
               .on("click", function(d) {
                    drawChart(d)
		            xScale.domain([d.x, d.x + d.dx]);
		            yScale.domain([d.y, d.y + d.dy]);
               })*/;

            svg.datum(treeData)
               .selectAll("text")
               .data(treeMap.nodes)
               .enter()
               .append("text")
               .text(function(d) { return d.children ? null : d[nameField]; })
               .attr("class", "text")
               .attr("x", function(d) {
                   return xScale(d.x) + padding + 4
               })
               .attr("y", function(d) {
                   return yScale(d.y) + padding + 12
               })
               .attr("width", function(d) {
                   return xScale(d.dx)
               })
               .attr("height", function(d) {
                   return yScale(d.dy)
               });
          }
        }
    };
}