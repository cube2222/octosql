import gulp from 'gulp';
import run from 'gulp-run-command';
 
/*
    "bundle": "webpack --config ./webpack.config.js",
    "clean": "rm -rf dist/*",
    "dev": "webpack-dev-server --config ./webpack.config.js --host 0.0.0.0",
    "dist": "cross-env NODE_ENV=production yarn bundle",
    "lint": "run-p lint:scss lint:ts",
    "lint:scss": "sass-lint",
    "lint:ts": "ts-lint",
    "lint-fix": "ts-lint --fix",
    "test": "exit 0",
    "verify": "run-p dist lint"
*/

gulp.task('clean', run('rm -rf dist/*'))
 
gulp.task('compile-docs', run('node ./docs-data/compile-docs-data.js', {
    cwd: './src',
}));

gulp.task('build-docs', run('node ./node_modules/webpack/bin/webpack.js --config ./webpack.config.js', {
    env: { NODE_ENV: 'production' },
}));

gulp.task('dev-docs', run('node ./node_modules/webpack-dev-server/bin/webpack-dev-server.js --config ./webpack.config.js --host 0.0.0.0', {
}));

gulp.task('dev', gulp.series(
    'compile-docs',
    'dev-docs',
));

gulp.task('build', gulp.series(
    'clean',
    'compile-docs',
    'build-docs',
));

gulp.task('default', gulp.series('build'));