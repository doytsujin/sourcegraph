import {EventLogger} from "./EventLogger";
import * as _ from "lodash";
import * as marked from "marked";
import {style} from "typestyle";

// tslint:disable-next-line
const truncate = require("html-truncate");

export type TooltipData = {title: string, doc?: string} | null;

const tooltipClassName = style({
	backgroundColor: "#2D2D30",
	maxWidth: "500px",
	maxHeight: "250px",
	border: "solid 1px #555",
	fontFamily: `"Helvetica Neue", Helvetica, Arial, sans-serif`,
	color: "rgba(213, 229, 242, 1)",
	fontSize: "12px",
	zIndex: 100,
	position: "absolute",
	overflow: "auto",
	padding: "5px 5px",
});

const tooltipTitleStyle = style({
	fontFamily: `Menlo, Monaco, Consolas, "Courier New", monospace`,
	wordWrap: "break-all",
});

const tooltipDocStyle = style({
	borderTop: "1px solid rgba(256, 256, 256, .8)",
	marginTop: "5px",
	paddingTop: "10px",
	fontFamily: `"Helvetica Neue", Helvetica, Arial, sans-serif`,
	wordWrap: "break-all",
});

let tooltip;
let loadingTooltip;

/**
 * createTooltips initializes the DOM elements used for the hover
 * tooltip and "Loading..." text indicator, adding the former
 * to the DOM (but hidden). It is idempotent.
 */
export function createTooltips(): void {
	if (tooltip || loadingTooltip) {
		return; // idempotence
	}

	tooltip	= document.createElement("DIV");
	tooltip.className = tooltipClassName;
	tooltip.classList.add("sg-tooltip");
	tooltip.style.visibility = "hidden";
	document.body.appendChild(tooltip);

	loadingTooltip = document.createElement("DIV");
	loadingTooltip.appendChild(document.createTextNode("Loading..."));
};

let target: HTMLElement | null = null;
let hoverEventProps: Object | null = null;

/**
 * setContext registers the active target (element) being moused over, as well
 * as properties to send to the event logger on when the tooltip is shown.
 */
export function setContext(activeTarget: HTMLElement | null, loggingStruct: Object | null): void {
	target = activeTarget;
	hoverEventProps = loggingStruct;
}

/**
 * clearContext removes all registered tooltip state and hides the tooltip.
 */
export function clearContext(): void {
	setContext(null, null);
	setTooltip(null, null);
	hideTooltip();
}

let currentTooltipText: string | null = null;
let currentTooltipDoc: string | null = null;
let isLoading = false; // whether the tooltip should show "Loading..." text

let loadingTimer: NodeJS.Timer; // a handle to a timeout which sets the "Loading..." text indicator

/**
 * clearLoading clears the "Loading..." tooltip, as well as any timeout
 * which would show the loading text.
 */
function clearLoading(): void {
	if (loadingTimer) {
		clearTimeout(loadingTimer);
	}
	isLoading = false;
}

/**
 * queueLoading shows the "Loading..." tooltip after 0.5 seconds.
 */
export function queueLoading(): void {
	clearLoading();
	loadingTimer = setTimeout(() => {
		isLoading = true;
		updateTooltip(target);
	}, 500);
}

/**
 * setTooltip shows the provided tooltip text (or hides the tooltip, if a null
 * argument is provided). It overrides the "Loading..." tooltip.
 */
export function setTooltip(data: TooltipData, t: HTMLElement | null): void {
	clearLoading();
	if (!data) {
		currentTooltipText = null;
	} else {
		currentTooltipText = data.title;
		currentTooltipDoc = data.doc || null;
	}
	updateTooltip(t);
}

/**
 * hideTooltip makes the tooltip on the DOM invisible.
 */
export function hideTooltip(): void {
	if (tooltip.firstChild) {
		tooltip.removeChild(tooltip.firstChild);
	}
	tooltip.style.visibility = "hidden"; // prevent black dot of empty content
}

/**
 * _updateTooltip displays the appropriate tooltip given current state (and may hide
 * the tooltip if no text is available).
 */
function _updateTooltip(activeTarget: HTMLElement | null): void {
	hideTooltip(); // hide before updating tooltip text

	if (!activeTarget) {
		// no activeTarget to show hover for; tooltip is hidden
		return;
	}

	if (!isLoading) {
		if (!currentTooltipText) {
			// no tooltip text for hovered token; tooltip is hidden
			return;
		}

		const tooltipText = document.createElement("DIV");
		tooltipText.className = tooltipTitleStyle;
		tooltipText.appendChild(document.createTextNode(currentTooltipText));
		tooltip.appendChild(tooltipText);

		if (currentTooltipDoc) {
			const tooltipDoc = document.createElement("DIV");
			tooltipDoc.className = tooltipDocStyle;
			tooltipDoc.innerHTML = truncate(marked(currentTooltipDoc, {gfm: true, breaks: true, sanitize: true}), 300);
			tooltip.appendChild(tooltipDoc);
		}

		// only log when displaying a real tooltip (not a loading indicator)
		EventLogger.logEventForCategory("Def", "Hover", "HighlightDef", hoverEventProps || undefined); // TODO(john): make hover event props invariant?
	} else {
		tooltip.appendChild(loadingTooltip);
	}

	if (!isLoading && currentTooltipText) {
		activeTarget.style.cursor = "pointer";
		if (!activeTarget.className.includes("sg-clickable")) {
			activeTarget.className = `${activeTarget.className} sg-clickable`;
		}
	}

	// Anchor it horizontally, prior to rendering to account for wrapping
	// changes to vertical height if the tooltip is at the edge of the viewport.
	const activeTargetBound = activeTarget.getBoundingClientRect();
	tooltip.style.left = (activeTargetBound.left + window.scrollX) + "px";

	// Anchor the tooltip vertically.
	const tooltipBound = tooltip.getBoundingClientRect();
	tooltip.style.top = (activeTargetBound.top - (tooltipBound.height + 5) + window.scrollY) + "px";

	// Make it all visible to the user.
	tooltip.style.visibility = "visible";
}

const updateTooltip = _.throttle(_updateTooltip, 50, { leading: true, trailing: true }); // prevent tooltip flashes as cursor moves quickly across the page
