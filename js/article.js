function figure1(element) {

  var url = "https://raw.githubusercontent.com/DanteLore/mot-data-in-spark/master/vis/results/passRateByAgeAndMileageBand.json";

  var resolution = 25

  var margin = {top: 10, right: 10, bottom: 50, left: 50},
      width = 540 - margin.left - margin.right,
      height = 540 - margin.top - margin.bottom;

  var x = d3.scale.linear().range([0, width]);
  var y = d3.scale.linear().range([height, 0]);
  var c = d3.scale.linear().range(["#ff0000", "#00ff00"]);

  var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom")
      .ticks(10);

  var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left")
      .ticks(10);

  var svg = d3.select(element).append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", 
            "translate(" + margin.left + "," + margin.top + ")");
          
  d3.json(url, function(error, rawData) {
    
    data = rawData
        .filter(function(d) { return d.age <= resolution && d.age > 0 })
        .filter(function(d) { return d.mileage <= (resolution + 1) * 10000 && d.mileage > 0 })
        .sort(function(a, b) { if(a.age == b.age) return a.mileage - b.mileage; else return a.age - b.age; });
    
    x.domain([d3.min(data, function(d) { return d.age; }), d3.max(data, function(d) { return d.age; }) + 1]);
    y.domain([0, d3.max(data, function(d) { return d.mileage; })]);
    c.domain([d3.min(data, function(d) { return d.rate; }), d3.max(data, function(d) { return d.rate; })]);
        
    svg.append("svg:g")
        .selectAll("g")
        .data(data)
        .enter()
        .append("rect")
        .attr("x", function(d) {
            return x(d.age);
        })
        .attr("y", function(d) {
            return y(d.mileage) - 1;
        })
        .attr("height", function(d) {
            return (height / resolution);
        })
        .attr("width", function(d) {
            return (width / resolution);
        })
        .style("fill", function(d) {
            return c(d.rate);
        })
        .style("stroke", function(d) {
                return c(d.rate);
        });
        
    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .selectAll("text")
        .style("text-anchor", "end")
        .attr("dx", "-.8em")
        .attr("dy", "-.55em")
        .attr("transform", "rotate(-90)" );
    
    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end");
  });
}
