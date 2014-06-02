var fs = require('fs');
var readme = fs.readFileSync('./README.md', 'utf8');

module.exports = function(grunt) {

	// Project configuration.
	grunt.initConfig({
		jsdoc : {
			dist : {
				src: ['lib', 'README.md'],
				options: {
					destination: 'docs',
					configure: 'conf.jsdoc.json'
				}
			}
		}
	});

	grunt.loadNpmTasks('grunt-jsdoc');

	grunt.registerTask('default', 'readme', function() {
		console.log(readme);
	});

	grunt.registerTask('build', 'build', function() {
		grunt.task.run('jsdoc');
	});

};
