'use strict';

module.exports = function(grunt) {
    // Time how long tasks take. Can help when optimizing build times
    require('time-grunt')(grunt);

    // Load grunt tasks automatically
    require('load-grunt-tasks')(grunt);

    var pkg = require('./package.json');
    var dirs = pkg.config.directories;

    // Define the configuration for all the tasks
    grunt.initConfig({
        // Project settings
        dirs: pkg.config.directories,

        // Watches files for changes and runs tasks based on the changed files
        watch: {
            js: {
                files: ['<%= dirs.app %>/js/**/*.js'],
                tasks: ['check-js', 'newer:copy:dev', 'wiredep:dev'],
                options: {
                    livereload: true
                }
            },
            gruntfile: {
                files: ['Gruntfile.js']
            },
            less: {
                files: ['<%= dirs.app %>/styles/**/*.{less,css}'],
                tasks: ['make-css:dev', 'wiredep:dev']
            },
            html: {
                files: ['<%= dirs.app %>/**/*.html'],
                tasks: ['newer:copy:dev', 'wiredep:dev']
            },
            livereload: {
                options: {
                    livereload: '<%= connect.options.livereload %>'
                },
                files: [
                    '<%= dirs.dev %>/**/*.*',
                ]
            }
        },

        // Empties folders to start fresh
        clean: {
            dist: {
                files: [{
                    dot: true,
                    src: [
                        '<%= dirs.dev %>',
                        '<%= dirs.dist %>/*',
                        '!<%= dirs.dist %>/.git*'
                    ]
                }]
            },
            dev: '<%= dirs.dev %>'
        },

        // Make sure code styles are up to par and there are no obvious mistakes
        jshint: {
            options: {
                jshintrc: '.jshintrc',
                reporter: require('jshint-stylish')
            },
            all: [
                'Gruntfile.js',
                '<%= dirs.app %>/js/**/*.js'
            ]
        },

        jscs: {
            options: {
                config: '.jscsrc'
            },
            all: [
                'Gruntfile.js',
                '<%= dirs.app %>/js/**/*.js'
            ]
        },

        uglify: {
            dist: {
                files: {
                    '<%= dirs.dist %>/scripts/scripts.js': [
                        '<%= dirs.dist %>/scripts/scripts.js'
                    ]
                }
            }
        },

        // Compiles less to CSS and generates necessary files if requested
        less: {
            options: {
                sourceMap: true
            },
            dist: {
                files: {
                    '<%= dirs.dist %>/styles/main.css':
                        '<%= dirs.app %>/styles/less/styles.less'
                }
            },
            dev: {
                files: [{
                    src: '<%= dirs.app %>/styles/less/styles.less',
                    dest: '<%= dirs.dev %>/styles/main.css'
                }]
            }
        },

        // Add vendor prefixed styles
        autoprefixer: {
            options: {
                browsers: [
                    '> 1%',
                    'last 2 versions',
                    'Firefox ESR',
                    'Opera 12.1'
                ]
            },
            dist: {
                files: [{
                    expand: true,
                    cwd: '<%= dirs.dist %>/styles/',
                    src: '**/*.css',
                    dest: '<%= dirs.dist %>/styles/'
                }]
            },
            dev: {
                files: [{
                    expand: true,
                    cwd: '<%= dirs.dev %>/styles/',
                    src: '**/*.css',
                    dest: '<%= dirs.dev %>/styles/'
                }]
            }
        },

        // The following *-min tasks produce minified files in the dist folder
        cssmin: {
            dist: {
                files: [{
                    expand: true,
                    cwd: '<%= dirs.dist %>/styles/',
                    src: '**/*.css',
                    dest: '<%= dirs.dist %>/styles/'
                }]
            }
        },

        imagemin: {
            dist: {
                files: [{
                    expand: true,
                    cwd: '<%= dirs.app %>/images',
                    src: '**/*.{gif,jpeg,jpg,png}',
                    dest: '<%= dirs.dist %>/images'
                }]
            }
        },

        wiredep: {
            dist: {
                directory: '<%= dirs.dist %>/vendors',
                src: [
                    '<%= dirs.dist %>/index.html',
                ],
                options: {
                }
            },
            dev: {
                directory: '<%= dirs.dev %>/vendors',
                src: [
                    '<%= dirs.dev %>/index.html',
                ],
                options: {
                }
            }
        },

        // Copies remaining files to places other tasks can use
        copy: {
            dist: {
                files: [{
                    expand: true,
                    dot: true,
                    cwd: '<%= dirs.app %>',
                    dest: '<%= dirs.dist %>',
                    src: [
                        '*.{ico,png,txt}',
                        '**/*.html',
                        'js/**/*.*',
                        'styles/fonts/**/*.*'
                    ]
                }, {
                    src: 'vendors/**/*.*',
                    dest: '<%= dirs.dist %>/'
                }, {
                    expand: true,
                    flatten: true,
                    src: 'vendors/font-awesome/fonts/**/*.*',
                    dest: '<%= dirs.dist %>/fonts/'
                }]
            },
            dev: {
                files: [{
                    expand: true,
                    dot: true,
                    cwd: '<%= dirs.app %>',
                    dest: '<%= dirs.dev %>',
                    src: [
                        '*.{ico,png,txt}',
                        '**/*.html',
                        'js/**/*.*',
                        'styles/fonts/**/*.*',
                        'img/**/*.{gif,jpeg,jpg,png}'
                    ]
                }, {
                    src: 'vendors/**/*.*',
                    dest: '<%= dirs.dev %>/'
                }, {
                    expand: true,
                    flatten: true,
                    src: 'vendors/font-awesome/fonts/**/*.*',
                    dest: '<%= dirs.dev %>/fonts/'
                }]
            }
        },

        // Run some tasks in parallel to speed up build process
        concurrent: {
            dev: [
                'check-js',
                'make-css:dev',
                'copy:dev'
            ],
            dist: [
                'check-js',
                'make-css:dist',
                'imagemin',
                'copy:dist'
            ]
        },

        // The actual grunt server settings
        connect: {
            options: {
                port: 9000,
                open: true,
                livereload: 35729,
                // Change this to '0.0.0.0' to access the server from outside
                hostname: 'localhost'
            },
            dev: {
                options: {
                    middleware: function(connect) {
                        return [
                            connect.static(dirs.dev)
                        ];
                    }
                }
            }
        }
    });

    grunt.registerTask('make-css:dev', [
        'less:dev',
        'autoprefixer:dev'
    ]);

    grunt.registerTask('make-css:dist', [
        'less:dist',
        'autoprefixer:dist',
        'cssmin:dist'
    ]);

    grunt.registerTask('check-js', [
        // 'jshint',
        // 'jscs'
    ]);

    grunt.registerTask('dev', function(target) {
        if (target !== 'watch') {
            grunt.task.run([
                'clean:dev',
                'concurrent:dev',
                'wiredep:dev',
            ]);
        }

        grunt.task.run([
            'connect:dev',
            'watch'
        ]);
    });

    grunt.registerTask('dist', [
        'clean:dist',
        'concurrent:dist',
        'wiredep:dist',
    ]);
};
