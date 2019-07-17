/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
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

import React from "react";

export const NavIcon: React.SFC<{ route: string }> = ({ route }) => {
    return (
        ICON_CONTENTS[route]
    );
};

const OPACITY = 0.6;
const ICON_CONTENTS: Record<string, JSX.Element> = {
    octosql: (
        <svg width="24" height="24" version="1.1" viewBox="0 0 40.392155 29.307758" xmlns="http://www.w3.org/2000/svg" xmlns:cc="http://creativecommons.org/ns#" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:xlink="http://www.w3.org/1999/xlink">
            <defs>
                <linearGradient id="linearGradient879">
                    <stop stop-color="#3870ac" offset="0"/>
                    <stop stop-color="#005bec" offset="1"/>
                </linearGradient>
                <linearGradient id="linearGradient881" x1="42.201" x2="48.022" y1="65.357" y2="44.984" gradientTransform="matrix(1.0154 0 0 .97298 -18.885 -39.423)" gradientUnits="userSpaceOnUse" xlink:href="#linearGradient879"/>
                <linearGradient id="linearGradient886" x1="44.133" x2="48.022" y1="55.138" y2="44.984" gradientTransform="matrix(.0042033 .49808 -.41992 .0035437 58.658 9.5102)" gradientUnits="userSpaceOnUse" xlink:href="#linearGradient879"/>
                <linearGradient id="linearGradient914" x1="44.133" x2="48.022" y1="55.138" y2="44.984" gradientTransform="matrix(-.0042033 .49808 .41992 .0035437 -12.897 6.7848)" gradientUnits="userSpaceOnUse" xlink:href="#linearGradient879"/>
            </defs>
            <g transform="translate(-3.7765 -3.0991)">
                <path d="m31.688 17.071c0 0.5861-0.12213 1.8789-0.21776 2.4248-0.5589 3.1907-2.1411 5.658-3.2421 7.8745-1.2475 2.5115-1.5795 4.5471-2.1919 4.1912-3.2757-1.904-7.851-5.9357-7.851-13.829 0-7.8933 3.0994-14.292 6.9226-14.292 3.8233 0 6.5802 5.7373 6.5802 13.631z" fill="url(#linearGradient881)" opacity=".793" stroke-width="0"/>
                <g fill="none" stroke="#000500" stroke-width=".836">
                    <path d="m16.733 20.468c-0.14554-1.8057-0.09558-3.8649 0.02633-5.9873 0.36113-6.287 3.9878-11.192 8.1004-10.956 4.1126 0.23623 7.1537 5.5244 6.7926 11.811l-2e-6 3e-6c-0.1789 3.1147-0.64386 6.999-1.9694 9.4658"/>
                    <g>
                        <path d="m17.819 24.831c0.61198 2.0566 2.7951 4.1363 1.3376 5.8582-2.6627 3.1454-13.528-6.5576-13.528-6.5576"/>
                        <path d="m42.911 26.592s-12.984 7.0491-14.737 5.0344c-0.08868-0.10194 4.5293-7.8895 6.2728-10.785"/>
                        <path d="m29.683 24.802-3.6918 6.718"/>
                    </g>
                    <path d="m17.819 24.831c-0.42263-1.6592 0.47799-3.7885 2.5798-3.7967 1.7629-0.0069 2.6535 1.4566 2.6277 2.9054-0.0025 0.13941-1.2683 0.64775-2.4719 1.1338"/>
                    <path d="m29.683 24.802c0.42263-1.6592-0.47799-3.7885-2.5798-3.7967-1.7629-0.0069-2.6536 1.4566-2.6277 2.9054 0.0047 0.26394 4.5378 1.8503 4.6036 2.1043"/>
                </g>
                <path d="m42.911 26.592s-5.7969 2.917-7.6204 3.7982c-1.2643 0.6109-4.3379 1.4811-4.3379 1.4811-1.2173 0.25997-2.7439-0.06774-2.7439-0.06774 1.8701-5.9568 5.025-12.237 5.025-12.237s3.5393 3.8677 9.6773 7.0254z" fill="url(#linearGradient886)" opacity=".201" stroke-width="0"/>
                <path d="m2.7182 22.809s5.9292 3.9754 7.7527 4.8565c1.2643 0.6109 6.653 3.4654 6.653 3.4654 1.2173 0.25997 2.5455-1.5229 2.5455-1.5229-1.8701-5.9568-5.6864-10.716-5.6864-10.716s-5.1268 0.75882-11.265 3.9166z" fill="url(#linearGradient914)" opacity=".201" stroke-width="0"/>
            </g>
        </svg>
    ),
};
