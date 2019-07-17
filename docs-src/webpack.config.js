/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { baseConfig } = require("@blueprintjs/webpack-build-scripts");
const CopyWebpackPlugin = require("copy-webpack-plugin");
const path = require("path");
const { CheckerPlugin } = require('awesome-typescript-loader');
const webpack = require('webpack');

module.exports = Object.assign({}, baseConfig, {
    entry: {
        "docs-app": [
            "./src/index.tsx",
            "./src/index.scss"
        ],
    },
    //context: path.resolve(__dirname, '.'),
    
    resolve: {
        extensions: ['.ts', '.tsx', '.js', '.jsx', 'index.ts', 'index.tsx', 'index.js', 'index.jsx'],
        alias: {},
    },

    output: {
        filename: "[name].js",
        path: path.resolve(__dirname, "./dist"),
    },

    module: {
        rules: (baseConfig.module.rules || []).concat([
          /*{
            test: /\.ts|\.tsx$/,
            enforce: 'pre',
            use: [
               {
                  loader: 'tslint-loader',
                  options: {
                      configFile: 'tslint.json',
                      typeCheck: true,
                      tsConfigFile: 'tsconfig.json',
                      emitErrors: true,
                      failOnHint: true,
                      fix: true
                  }
               }
            ]
          },*/
          {
            test: /\.js$/,
            use: ['babel-loader', 'source-map-loader'],
            exclude: /node_modules/,
          },
          {
            test: /\.tsx?$/,
            use: ['babel-loader', 'awesome-typescript-loader'],
          },
          {
            test: /\.(jpe?g|png|gif|svg)$/i,
            loaders: [
              'file-loader?hash=sha512&digest=hex&name=img/[hash].[ext]',
              'image-webpack-loader?bypassOnDebug&optipng.optimizationLevel=7&gifsicle.interlaced=false',
            ],
          },
        ])
    },
    
    plugins: baseConfig.plugins.concat([
        new CheckerPlugin(),
        new CopyWebpackPlugin([
            // to: is relative to dist/
            { from: "src/index.html", to: "." },
            { from: "src/assets/favicon.png", to: "assets" },
        ]),
        new webpack.NormalModuleReplacementPlugin(/@blinkforms\/core/, function(resource) {
            resource.request = resource.request.replace(/@blinkforms\/core/, path.resolve(__dirname, path.join('.', 'blinkforms', 'typescript-core')));
        }),
        new webpack.NormalModuleReplacementPlugin(/@blinkforms\/react/, function(resource) {
            resource.request = resource.request.replace(/@blinkforms\/react/, path.resolve(__dirname, path.join('.', 'blinkforms', 'react-json-blinkforms', 'bin', 'index.js')));
        }),
    ]),
});
