var motApp = angular.module('motApp', ['ngRoute']);

var createCdf = function(data) {
    var values = data.map(function(d) { return d.count; })
    var sum = d3.sum(values);
    var running = 0
    data.forEach(function(d) {
        running += d.count / sum;
        d.cdf = Math.min(1, running);
        return d
    })
    return data
}

motApp
	.controller('MotController', function($scope, $http) {

        $http.get("results/passRateByAgeBand.json").success(function(data) {
            var sorted = data.sort(function(a, b) { return a.age - b.age; })
            $scope.passRate100 = sorted
            $scope.passRate20 = sorted.slice(0, 21)
            var count = 0
            sorted.forEach(function (row) { count += row.count; })
            $scope.recordCount = count.toLocaleString()
            cdf = createCdf(sorted)
            $scope.cdf100 = cdf
            $scope.cdf20 = cdf.slice(0, 21)
        })

        $http.get("results/motTestsByVehicleColour.json").success(function(data) {
            var colours = data.sort(function(a, b) { return b.count - a.count })
            $scope.colours = colours.map(function(x) {
                if(["not stated", "multi-colour"].indexOf(x.colour) >= 0) {
                    x.barColour = "white";
                }
                else if(x.colour == "cream") {
                    x.barColour = "#FFFACD";
                }
                else {
                    x.barColour = x.colour;
                }
                return x;
            })
        })

	})
	.directive("categoryBarChart", categoryBarChartDirective)
	.directive("lineChart", lineChartDirective)
	.directive("columnChart", columnChartDirective)