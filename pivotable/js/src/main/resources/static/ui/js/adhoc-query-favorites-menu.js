// @ts-check
//
// Single-button dropdown menu that consolidates the two previously-separate favorite affordances
// ("Save current as Favorite" via <AdhocQueryFavorite> and "Browse Favorites" via
// <AdhocQueryFavorites>) into one Bootstrap dropdown — same visual idiom the DrillThrough cell
// modal uses for its action menu.
//
// Mechanism:
//   - The two existing components are still mounted (so their modals stay in the DOM) but with
//     `hideTrigger=true` to suppress their own buttons.
//   - This wrapper renders ONE dropdown-toggle button labelled "Favorites" (with the existing
//     unsaved-changes `*` indicator) and a two-item menu whose entries open the same modal
//     elements via `data-bs-toggle="modal" data-bs-target="#queryFavorite|#queryFavorites"`.
//
// Bootstrap quirk: a `dropdown-item` can carry `data-bs-toggle="modal"` AND `data-bs-target` and
// the dropdown closes / the modal opens in one click. Same pattern as the DrillThrough menu.

import AdhocQueryFavorite from "./adhoc-query-favorite.js";
import AdhocQueryFavorites from "./adhoc-query-favorites.js";

import { usePreferencesStore } from "./store-preferences.js";

export default {
	components: {
		AdhocQueryFavorite,
		AdhocQueryFavorites,
	},
	props: {
		queryModel: {
			type: Object,
			required: true,
		},
	},
	setup(props) {
		const preferencesStore = usePreferencesStore();

		return {
			preferencesStore,
		};
	},
	template: /* HTML */ `
		<div class="btn-group">
			<button
				type="button"
				class="btn btn-primary btn-sm dropdown-toggle"
				data-bs-toggle="dropdown"
				aria-expanded="false"
				:title="preferencesStore.hasUnsavedChanges(queryModel) ? 'Favorites (current query has unsaved changes)' : 'Favorites'"
			>
				<i class="bi bi-star me-1" aria-hidden="true"></i>
				Favorites
				<!--
					Unsaved-changes indicator preserved from the standalone AdhocQueryFavorite trigger so
					users don't lose the visual cue that the current queryModel diverges from the saved
					version they last loaded. Same asterisk glyph, same condition (preferencesStore.hasUnsavedChanges).
				-->
				<span v-if="preferencesStore.hasUnsavedChanges(queryModel)"> *</span>
			</button>
			<ul class="dropdown-menu shadow-sm">
				<li>
					<button type="button" class="dropdown-item" data-bs-toggle="modal" data-bs-target="#queryFavorite">
						<i class="bi bi-bookmark-plus me-1" aria-hidden="true"></i>
						Save current as Favorite
						<span v-if="preferencesStore.hasUnsavedChanges(queryModel)" class="text-warning ms-1">*</span>
					</button>
				</li>
				<li>
					<button type="button" class="dropdown-item" data-bs-toggle="modal" data-bs-target="#queryFavorites">
						<i class="bi bi-bookmark-star me-1" aria-hidden="true"></i>
						Browse Favorites
					</button>
				</li>
			</ul>
		</div>

		<!--
			The underlying components are still rendered (no UI of their own, but they own the
			Teleport-to-body modal elements that the dropdown items target). hideTrigger keeps
			their original buttons out of the DOM so we don't end up with three triggers for two
			modals.
		-->
		<AdhocQueryFavorite :queryModel="queryModel" :hideTrigger="true" />
		<AdhocQueryFavorites :queryModel="queryModel" :hideTrigger="true" />
	`,
};
